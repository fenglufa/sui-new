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

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use sui_types::base_types::ObjectID;
use sui_types::base_types::SuiAddress;
use sui_types::object::Object;
use sui_types::object::Owner;
use tokio::io::AsyncWriteExt;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Frames queued ahead of the writer task. When this fills up the commit path is
/// faster than the consumer, so frames are dropped rather than applied as back
/// pressure: a stale cache is recovered by the next push, a stalled validator is not.
const QUEUE_DEPTH: usize = 1024;

/// Write deadline per connection. A client that stalls past this is disconnected
/// instead of holding the writer task (and every other client) behind it.
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// Warn about dropped frames at most this often, so a persistently slow consumer
/// does not flood the log.
const DROP_LOG_INTERVAL: u64 = 1000;

/// Owns the socket and the task that drains it. Cheap to hand out, but only ever
/// constructed once per node process.
#[derive(Debug)]
pub struct CacheUpdateHandler {
    frames: mpsc::Sender<Vec<(ObjectID, Object)>>,
    /// Live client count, checked before queueing so an idle node does no work.
    connections: Arc<AtomicUsize>,
    dropped_frames: Arc<AtomicU64>,
}

impl CacheUpdateHandler {
    /// Bind `socket_path` and start the writer task.
    ///
    /// A bind failure is logged and leaves the handler inert. This runs during node
    /// startup, where aborting the node over an optional push channel would be worse
    /// than not offering it.
    pub fn new(socket_path: PathBuf) -> Self {
        let (frames_tx, frames_rx) = mpsc::channel(QUEUE_DEPTH);
        let (incoming_tx, incoming_rx) = mpsc::channel(QUEUE_DEPTH);
        let connections = Arc::new(AtomicUsize::new(0));
        let dropped_frames = Arc::new(AtomicU64::new(0));

        let listener = match bind_socket(&socket_path) {
            Ok(listener) => listener,
            Err(e) => {
                warn!(
                    path = %socket_path.display(),
                    error = %e,
                    "cache update socket unavailable, object push disabled"
                );
                return Self {
                    frames: frames_tx,
                    connections,
                    dropped_frames,
                };
            }
        };

        info!(path = %socket_path.display(), "listening for cache update subscribers");

        let accept_connections = Arc::clone(&connections);
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _addr)) => {
                        accept_connections.fetch_add(1, Ordering::Relaxed);
                        info!("cache update subscriber connected");
                        if incoming_tx.send(stream).await.is_err() {
                            // Writer task is gone, so nothing can serve this client.
                            break;
                        }
                    }
                    Err(e) => {
                        // Accept errors are transient on some platforms (EMFILE, signal
                        // interrupts). Back off and keep listening rather than tearing
                        // down the push path for the whole process lifetime.
                        error!(error = %e, "error accepting cache update connection");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
            debug!("cache update accept loop stopped");
        });

        let writer_connections = Arc::clone(&connections);
        let writer_dropped = Arc::clone(&dropped_frames);
        let writer_path = socket_path.clone();
        tokio::spawn(async move {
            run_writer(frames_rx, incoming_rx, writer_connections, writer_dropped).await;
            // The listener lives in the accept task, so the socket file is only
            // removable once the writer is done with it.
            let _ = std::fs::remove_file(&writer_path);
            debug!(path = %writer_path.display(), "cache update writer stopped");
        });

        Self {
            frames: frames_tx,
            connections,
            dropped_frames,
        }
    }

    /// Queue `objects` for delivery. Never blocks the caller.
    pub fn notify_written(&self, objects: Vec<(ObjectID, Object)>) {
        if objects.is_empty() || self.connections.load(Ordering::Relaxed) == 0 {
            return;
        }

        match self.frames.try_send(objects) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                let total = self.dropped_frames.fetch_add(1, Ordering::Relaxed) + 1;
                if total % DROP_LOG_INTERVAL == 1 {
                    warn!(
                        dropped_total = total,
                        "cache update queue full, dropping object push; subscriber is too slow"
                    );
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                debug!("cache update writer stopped, object push disabled");
            }
        }
    }
}

/// Remove a stale socket left by a previous run, then bind.
fn bind_socket(path: &Path) -> std::io::Result<UnixListener> {
    if path.exists() {
        let _ = std::fs::remove_file(path);
    }
    UnixListener::bind(path)
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
                        warn!(entry, error = %e, "ignoring invalid watched owner address");
                    }
                }
            }
            (!parsed.is_empty()).then_some(parsed)
        })
        .as_ref()
}

/// Single owner of every client connection, so writes are serialized without a lock
/// and one payload is serialized once no matter how many subscribers there are.
async fn run_writer(
    mut frames: mpsc::Receiver<Vec<(ObjectID, Object)>>,
    mut incoming: mpsc::Receiver<UnixStream>,
    connections: Arc<AtomicUsize>,
    dropped_frames: Arc<AtomicU64>,
) {
    let mut clients: Vec<UnixStream> = Vec::new();

    loop {
        tokio::select! {
            biased;

            maybe_stream = incoming.recv() => {
                match maybe_stream {
                    Some(stream) => clients.push(stream),
                    None => break,
                }
            }

            maybe_objects = frames.recv() => {
                let Some(objects) = maybe_objects else {
                    // Handler dropped: shut down and release the socket path.
                    break;
                };
                if clients.is_empty() {
                    // Nothing to deliver. Counted as dropped so an operator can tell
                    // the push path was active but had no audience.
                    dropped_frames.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                if deliver(&objects, &mut clients).await == 0 {
                    connections.store(0, Ordering::Relaxed);
                    continue;
                }
                connections.store(clients.len(), Ordering::Relaxed);
            }
        }
    }
}

/// Write one framed frame to every client, dropping those that fail or stall.
/// Returns how many clients are still live.
async fn deliver(objects: &[(ObjectID, Object)], clients: &mut Vec<UnixStream>) -> usize {
    let payload = match bcs::to_bytes(objects) {
        Ok(payload) => payload,
        Err(e) => {
            // Not a per-client problem; skip the frame for everyone.
            error!(error = %e, "failed to serialize cache update, skipping frame");
            return clients.len();
        }
    };
    let mut frame_bytes = Vec::with_capacity(4 + payload.len());
    frame_bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame_bytes.extend_from_slice(&payload);

    let mut retained = Vec::with_capacity(clients.len());
    for mut client in clients.drain(..) {
        let written = tokio::time::timeout(WRITE_TIMEOUT, async {
            client.write_all(&frame_bytes).await
        })
        .await;
        match written {
            Ok(Ok(())) => retained.push(client),
            Ok(Err(e)) => {
                info!(error = %e, "dropping cache update subscriber, write failed");
            }
            Err(_) => {
                warn!(
                    timeout_ms = WRITE_TIMEOUT.as_millis(),
                    "dropping cache update subscriber, write timed out"
                );
            }
        }
    }
    let live = retained.len();
    *clients = retained;
    live
}

#[cfg(test)]
mod tests {
    use super::*;
    use sui_types::base_types::SequenceNumber;
    use sui_types::digests::TransactionDigest;
    use sui_types::object::MoveObject;
    use tokio::io::AsyncReadExt;

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
            if handler.connections.load(Ordering::Relaxed) >= expected {
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
        assert_eq!(handler.connections.load(Ordering::Relaxed), 0);

        let a = coin(5, 500);
        handler.notify_written(vec![(a.id(), a)]);
        handler.notify_written(vec![]);
        // Neither call panicked or queued work, so the commit path is unaffected.
        assert_eq!(handler.connections.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn watched_owner_filter_matches_address_owned_objects_only_when_configured() {
        let a = coin(6, 600);
        // SUI_MEV_WATCHED_OWNERS is unset in the test environment, so nothing is
        // watched. This is the case that relay-patch turned into a panic on every
        // commit; here it must simply be false.
        assert!(!is_watched_object(&a));
    }
}
