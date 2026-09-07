// Push committed objects to out-of-process MEV simulators over a Unix socket.
//
// A simulator keeps a warm copy of the pool objects it trades against. Polling that
// copy costs round trips, so instead the node pushes every committed object the
// simulator has shown interest in (see `pool_related`).
//
// Wire format, unchanged from the relay-patch this replaces:
//
//     [ u32 LE: payload length ][ BCS Vec<(ObjectID, Object)> ]
//
// The bot reads it in `DBSimulator::spawn_update_thread` with `read_exact` +
// `bcs::from_bytes`, so the framing and byte order here are load-bearing.
//
// Socket plumbing, queueing and subscriber lifecycle live in `mev_socket`, shared
// with the transaction stream, which uses a different payload but must behave the
// same way toward the commit path.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::OnceLock;

use sui_types::base_types::ObjectID;
use sui_types::base_types::SuiAddress;
use sui_types::object::Object;
use sui_types::object::Owner;
use tracing::error;

use crate::mev_socket::SocketFanOut;

/// One socket per node process, handed out by reference from `AuthorityState`.
#[derive(Debug)]
pub struct CacheUpdateHandler {
    fanout: SocketFanOut<Vec<(ObjectID, Object)>>,
}

impl CacheUpdateHandler {
    /// Bind `socket_path` and start the writer task.
    ///
    /// A bind failure leaves the handler inert rather than panicking: this runs
    /// during node startup, where aborting the node over an optional push channel
    /// would be worse than not offering it.
    pub fn new(socket_path: PathBuf) -> Self {
        Self {
            fanout: SocketFanOut::new(
                socket_path,
                "cache update",
                |objects: &Vec<(ObjectID, Object)>| frame_cache_update(objects),
            ),
        }
    }

    /// Queue `objects` for delivery. Never blocks the caller.
    pub fn notify_written(&self, objects: Vec<(ObjectID, Object)>) {
        if objects.is_empty() || !self.fanout.has_subscribers() {
            return;
        }
        self.fanout.push(objects);
    }
}

/// Encode one frame. Returns `None` if the objects cannot be serialized, in which
/// case the writer skips the frame for every subscriber.
fn frame_cache_update(objects: &[(ObjectID, Object)]) -> Option<Vec<u8>> {
    let payload = match bcs::to_bytes(objects) {
        Ok(payload) => payload,
        Err(e) => {
            error!(error = %e, "failed to serialize cache update, skipping frame");
            return None;
        }
    };
    let mut frame_bytes = Vec::with_capacity(4 + payload.len());
    frame_bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame_bytes.extend_from_slice(&payload);
    Some(frame_bytes)
}

/// Comma-separated addresses whose newly written objects are always pushed, on top of
/// the self-learned pool set.
const OWNERS_ENV: &str = "SUI_MEV_WATCHED_OWNERS";

/// Whether a committed object is worth pushing beyond the pool set.
///
/// This replaces relay-patch's `BRITISHBROADCASTCORPORATION` check, which was broken
/// twice over: it `.expect()`ed the env var on the commit path of every non-system
/// transaction, and it compared `Owner` against an `ObjectID`, a comparison upstream
/// only satisfies for `Owner::ObjectOwner` — so for a simulator's address-owned coins
/// it was always false.
pub(crate) fn is_watched_object(object: &Object) -> bool {
    let Some(owners) = watched_owners() else {
        return false;
    };
    matches!(object.owner(), Owner::AddressOwner(address) if owners.contains(address))
}

/// Parsed once per process. Unset or entirely malformed yields `None`, which costs a
/// single relaxed load per written object.
fn watched_owners() -> Option<&'static HashSet<SuiAddress>> {
    static OWNERS: OnceLock<Option<HashSet<SuiAddress>>> = OnceLock::new();
    OWNERS
        .get_or_init(|| {
            let raw = std::env::var(OWNERS_ENV).ok()?;
            let mut parsed = HashSet::new();
            for entry in raw.split(',') {
                let entry = entry.trim();
                if entry.is_empty() {
                    continue;
                }
                match entry.parse::<SuiAddress>() {
                    Ok(address) => {
                        parsed.insert(address);
                    }
                    Err(e) => {
                        tracing::warn!(entry, error = %e, "ignoring invalid watched owner address");
                    }
                }
            }
            (!parsed.is_empty()).then_some(parsed)
        })
        .as_ref()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;
    use sui_types::base_types::SequenceNumber;
    use sui_types::digests::TransactionDigest;
    use sui_types::object::MoveObject;
    use tokio::io::AsyncReadExt;
    use tokio::net::UnixStream;

    fn scratch_socket(label: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "sui_cache_update_test_{}_{}_{}.sock",
            std::process::id(),
            label,
            n
        ))
    }

    fn coin(n: u8, value: u64) -> Object {
        let id: ObjectID = format!("0x{n:0>64x}").parse().expect("valid object id");
        let owner: SuiAddress =
            "0x0000000000000000000000000000000000000000000000000000000000000001"
                .parse()
                .expect("valid address");
        Object::new_move(
            MoveObject::new_gas_coin(SequenceNumber::from(n as u64 + 1), id, value),
            Owner::AddressOwner(owner),
            TransactionDigest::genesis_marker(),
        )
    }

    /// Wait for the accept task to register `expected` subscribers, so a push is
    /// guaranteed to be queued rather than short-circuited by the zero-client check.
    async fn await_subscribers(handler: &CacheUpdateHandler, expected: usize) {
        for _ in 0..200 {
            if handler.fanout.subscriber_count() >= expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("subscriber never registered on the cache update socket");
    }

    /// Read exactly one frame, the way the bot's `spawn_update_thread` does.
    async fn read_one_frame(stream: &mut UnixStream) -> Vec<(ObjectID, Object)> {
        let mut len_buf = [0u8; 4];
        stream
            .read_exact(&mut len_buf)
            .await
            .expect("read length prefix");
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut payload = vec![0u8; len];
        stream.read_exact(&mut payload).await.expect("read payload");
        bcs::from_bytes(&payload).expect("frame decodes as BCS Vec<(ObjectID, Object)>")
    }

    #[tokio::test]
    async fn pushes_frames_in_the_format_the_bot_parses() {
        let path = scratch_socket("push");
        let handler = CacheUpdateHandler::new(path.clone());
        let mut client = UnixStream::connect(&path).await.expect("connect");
        await_subscribers(&handler, 1).await;

        let a = coin(1, 100);
        let b = coin(2, 200);
        handler.notify_written(vec![(a.id(), a.clone()), (b.id(), b.clone())]);

        let received = read_one_frame(&mut client).await;
        assert_eq!(received.len(), 2, "both objects should arrive");
        assert_eq!(received[0].0, a.id());
        assert_eq!(received[0].1.digest(), a.digest());
        assert_eq!(received[1].0, b.id());
        assert_eq!(received[1].1.digest(), b.digest());

        drop(handler);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn every_subscriber_receives_the_same_frame() {
        let path = scratch_socket("fanout");
        let handler = CacheUpdateHandler::new(path.clone());
        let mut first = UnixStream::connect(&path).await.expect("connect");
        let mut second = UnixStream::connect(&path).await.expect("connect");
        await_subscribers(&handler, 2).await;

        let a = coin(3, 300);
        handler.notify_written(vec![(a.id(), a.clone())]);

        for client in [&mut first, &mut second] {
            let received = read_one_frame(client).await;
            assert_eq!(received.len(), 1);
            assert_eq!(received[0].1.digest(), a.digest());
        }

        drop(handler);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn a_dead_subscriber_does_not_stop_the_live_one() {
        let path = scratch_socket("dead");
        let handler = CacheUpdateHandler::new(path.clone());
        let dead = UnixStream::connect(&path).await.expect("connect");
        let mut live = UnixStream::connect(&path).await.expect("connect");
        await_subscribers(&handler, 2).await;

        drop(dead);
        let a = coin(4, 400);
        handler.notify_written(vec![(a.id(), a.clone())]);

        let received = read_one_frame(&mut live).await;
        assert_eq!(received[0].1.digest(), a.digest());

        drop(handler);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn an_unbindable_socket_disables_push_instead_of_panicking() {
        let handler = CacheUpdateHandler::new(PathBuf::from("/nonexistent-dir/sui.sock"));
        assert_eq!(handler.fanout.subscriber_count(), 0);

        let a = coin(5, 500);
        handler.notify_written(vec![(a.id(), a)]);
        handler.notify_written(vec![]);
        // Neither call panicked or queued work, so the commit path is unaffected.
        assert_eq!(handler.fanout.subscriber_count(), 0);
    }

    #[test]
    fn watched_owner_filter_matches_address_owned_objects_only_when_configured() {
        let a = coin(6, 600);
        // SUI_MEV_WATCHED_OWNERS is unset in the test environment, so nothing is
        // watched. This is the case that relay-patch turned into a panic on every
        // commit; here it must simply be false.
        assert!(!is_watched_object(&a));
    }

    /// A frame the framer rejects must be skipped, not delivered as garbage and not
    /// taken down the whole channel with it.
    #[tokio::test]
    async fn an_unencodable_frame_is_skipped_without_losing_later_frames() {
        let path = scratch_socket("badframe");
        let fanout: SocketFanOut<Vec<(ObjectID, Object)>> = SocketFanOut::new(
            path.clone(),
            "test channel",
            |objects: &Vec<(ObjectID, Object)>| {
                if objects.is_empty() {
                    None
                } else {
                    frame_cache_update(objects)
                }
            },
        );

        let mut client = UnixStream::connect(&path).await.expect("connect");
        for _ in 0..200 {
            if fanout.subscriber_count() > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        fanout.push(vec![]);
        let a = coin(7, 700);
        fanout.push(vec![(a.id(), a.clone())]);

        let received = read_one_frame(&mut client).await;
        assert_eq!(received.len(), 1, "only the frame that encoded");
        assert_eq!(received[0].1.digest(), a.digest());

        drop(fanout);
        let _ = std::fs::remove_file(&path);
    }
}
