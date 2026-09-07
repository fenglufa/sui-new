// Push committed transaction effects and events to an out-of-process MEV bot over a
// Unix socket.
//
// The bot's `PublicTxCollector` turns these into the event stream its strategies run
// on, which is how it learns that a swap happened without polling fullnode for
// effects first.
//
// Wire format, unchanged from the relay-patch this replaces:
//
//     [ u32 BE: effects length ][ bincode TransactionEffects ]
//     [ u32 BE: events length  ][ serde_json Vec<SuiEvent>   ]
//
// Note that this differs from the object stream in `cache_update_handler`, which uses
// BCS behind a u32 LE length. The bot parses each socket separately, so both formats
// are load-bearing and neither is interchangeable with the other.
//
// The payload is produced during post-processing, where event type layouts have
// already been resolved for the node's own event subscriptions, rather than on the
// commit path where they would have to be resolved again. Socket plumbing lives in
// `mev_socket`.

use std::path::PathBuf;

use sui_json_rpc_types::SuiEvent;
use sui_types::effects::TransactionEffects;
use tracing::error;

use crate::mev_socket::SocketFanOut;

/// One committed transaction, queued and encoded together so a subscriber never sees
/// effects without their events.
#[derive(Debug)]
pub(crate) struct TxUpdate {
    effects: TransactionEffects,
    events: Vec<SuiEvent>,
}

/// One socket per node process, handed out by reference from `AuthorityState`.
#[derive(Debug)]
pub struct TxHandler {
    fanout: SocketFanOut<TxUpdate>,
}

impl TxHandler {
    /// Bind `socket_path` and start the writer task. A bind failure leaves the
    /// handler inert rather than panicking during node startup.
    pub fn new(socket_path: PathBuf) -> Self {
        Self {
            fanout: SocketFanOut::new(socket_path, "tx update", |update: &TxUpdate| {
                frame_tx_update(update)
            }),
        }
    }

    /// Queue one committed transaction for delivery. Never blocks the caller.
    ///
    /// Transactions without events are skipped: the bot consumes this stream for
    /// events, and the relay-patch this replaces gated the same way.
    pub fn notify_effects_events(&self, effects: &TransactionEffects, events: Vec<SuiEvent>) {
        if events.is_empty() || !self.fanout.has_subscribers() {
            return;
        }
        self.fanout.push(TxUpdate {
            effects: effects.clone(),
            events,
        });
    }
}

/// Encode one frame. Returns `None` if either half cannot be encoded, in which case
/// the writer skips the frame for every subscriber.
fn frame_tx_update(update: &TxUpdate) -> Option<Vec<u8>> {
    let effects_bytes = match bincode::serialize(&update.effects) {
        Ok(bytes) => bytes,
        Err(e) => {
            error!(error = %e, "failed to serialize tx effects, skipping frame");
            return None;
        }
    };
    let events_bytes = match serde_json::to_vec(&update.events) {
        Ok(bytes) => bytes,
        Err(e) => {
            error!(error = %e, "failed to serialize tx events, skipping frame");
            return None;
        }
    };

    let mut frame_bytes = Vec::with_capacity(8 + effects_bytes.len() + events_bytes.len());
    frame_bytes.extend_from_slice(&(effects_bytes.len() as u32).to_be_bytes());
    frame_bytes.extend_from_slice(&effects_bytes);
    frame_bytes.extend_from_slice(&(events_bytes.len() as u32).to_be_bytes());
    frame_bytes.extend_from_slice(&events_bytes);
    Some(frame_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;
    use sui_json_rpc_types::BcsEvent;
    use sui_types::base_types::ObjectID;
    use sui_types::digests::TransactionDigest;
    use sui_types::event::EventID;
    use tokio::io::AsyncReadExt;
    use tokio::net::UnixStream;

    fn scratch_socket(label: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "sui_tx_test_{}_{}_{}.sock",
            std::process::id(),
            label,
            n
        ))
    }

    fn swap_event() -> SuiEvent {
        SuiEvent {
            id: EventID {
                tx_digest: TransactionDigest::genesis_marker(),
                event_seq: 0,
            },
            package_id: ObjectID::ZERO,
            transaction_module: "pool".parse().expect("valid identifier"),
            sender: "0x0000000000000000000000000000000000000000000000000000000000000001"
                .parse()
                .expect("valid address"),
            type_: "0x5::pool::Swap".parse().expect("valid struct tag"),
            parsed_json: serde_json::json!({ "amount": 42u64 }),
            bcs: BcsEvent::new(vec![1, 2, 3]),
            timestamp_ms: Some(1_700_000_000_000),
        }
    }

    async fn await_subscribers(handler: &TxHandler, expected: usize) {
        for _ in 0..200 {
            if handler.fanout.subscriber_count() >= expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("subscriber never registered on the tx socket");
    }

    /// Read one frame the way the bot's `PublicTxCollector` does.
    async fn read_one_update(stream: &mut UnixStream) -> (TransactionEffects, Vec<SuiEvent>) {
        let mut len_buf = [0u8; 4];

        stream
            .read_exact(&mut len_buf)
            .await
            .expect("read effects length");
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut buf = vec![0u8; len];
        stream.read_exact(&mut buf).await.expect("read effects");
        let effects: TransactionEffects =
            bincode::deserialize(&buf).expect("effects decode as bincode");

        stream
            .read_exact(&mut len_buf)
            .await
            .expect("read events length");
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut buf = vec![0u8; len];
        stream.read_exact(&mut buf).await.expect("read events");
        let events: Vec<SuiEvent> =
            serde_json::from_slice(&buf).expect("events decode as a JSON array");

        (effects, events)
    }

    #[tokio::test]
    async fn pushes_effects_and_events_in_the_format_the_bot_parses() {
        let path = scratch_socket("push");
        let handler = TxHandler::new(path.clone());
        let mut client = UnixStream::connect(&path).await.expect("connect");
        await_subscribers(&handler, 1).await;

        let effects = TransactionEffects::default();
        let event = swap_event();
        handler.notify_effects_events(&effects, vec![event.clone()]);

        let (received_effects, received_events) = read_one_update(&mut client).await;
        assert_eq!(received_effects, effects, "effects round-tripped");
        assert_eq!(received_events.len(), 1);
        assert_eq!(received_events[0].type_, event.type_);
        assert_eq!(received_events[0].parsed_json, event.parsed_json);
        assert_eq!(received_events[0].bcs.bytes(), event.bcs.bytes());

        drop(handler);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn a_transaction_without_events_is_not_pushed() {
        let path = scratch_socket("noevents");
        let handler = TxHandler::new(path.clone());
        let mut client = UnixStream::connect(&path).await.expect("connect");
        await_subscribers(&handler, 1).await;

        handler.notify_effects_events(&TransactionEffects::default(), vec![]);

        let nothing =
            tokio::time::timeout(Duration::from_millis(200), client.read_exact(&mut [0u8; 4]))
                .await;
        assert!(
            nothing.is_err(),
            "an eventless transaction should produce no frame"
        );

        drop(handler);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn an_unbindable_socket_disables_push_instead_of_panicking() {
        let handler = TxHandler::new(PathBuf::from("/nonexistent-dir/sui_tx.sock"));
        assert_eq!(handler.fanout.subscriber_count(), 0);
        handler.notify_effects_events(&TransactionEffects::default(), vec![swap_event()]);
        assert_eq!(handler.fanout.subscriber_count(), 0);
    }
}
