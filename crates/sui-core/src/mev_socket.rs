// Shared Unix-socket fan-out for the node's MEV side channels.
//
// Two streams use this: committed pool objects (`cache_update_handler`) and committed
// transaction effects plus events (`tx_handler`). Both are optional, fire-and-forget
// channels hanging off the commit path, so they have to behave identically in the ways
// that matter: never block the producer, serialize once per frame regardless of
// subscriber count, evict a subscriber that stalls, and survive a bind failure.
//
// Frames are opaque here. `T` is whatever the producer already has on hand, and the
// `framer` turns it into the exact bytes a given subscriber protocol expects. Framing
// differences therefore stay in the callers, where the wire format is documented.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Frames queued ahead of the writer task. When this fills up the commit path is
/// faster than the consumer, so frames are dropped rather than applied as back
/// pressure: a stale cache is recovered by the next push, a stalled validator is not.
pub(crate) const QUEUE_DEPTH: usize = 1024;

/// Write deadline per connection. A client that stalls past this is disconnected
/// instead of holding the writer task (and every other client) behind it.
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// Warn about dropped frames at most this often, so a persistently slow consumer does
/// not flood the log.
const DROP_LOG_INTERVAL: u64 = 1000;

/// One socket plus the task that drains it. Cheap to hand out, but only ever
/// constructed once per node process per channel.
///
/// `Debug` is derived for the concrete instantiations used by the node, both of which
/// hold `Debug` payloads.
#[derive(Debug)]
pub(crate) struct SocketFanOut<T> {
    frames: mpsc::Sender<T>,
    /// Live client count, checked by producers before queueing so an idle node does
    /// no serialization work.
    ///
    /// Owned by the writer task: only `run_writer` ever writes it. It used to have two
    /// writers -- accept incremented it while the writer overwrote it with `store` --
    /// and an increment could be lost against a delivery that dropped every client.
    /// Producers gate on `has_subscribers`, so the erased subscriber was never handed
    /// a frame again, and the writer never ran to correct the count either: a live
    /// channel, permanently silent.
    connections: Arc<AtomicUsize>,
    dropped_frames: Arc<AtomicU64>,
    label: &'static str,
}

impl<T: Send + Sync + 'static> SocketFanOut<T> {
    /// Bind `socket_path` and start the writer task.
    ///
    /// `label` names this channel in logs, and `framer` produces the bytes to write.
    /// A `None` from `framer` means the frame could not be encoded and is skipped for
    /// every subscriber.
    ///
    /// A bind failure is logged and leaves the fan-out inert. This runs during node
    /// startup, where aborting the node over an optional push channel would be worse
    /// than not offering it.
    pub(crate) fn new(
        socket_path: PathBuf,
        label: &'static str,
        framer: impl Fn(&T) -> Option<Vec<u8>> + Send + Sync + 'static,
    ) -> Self {
        let (frames_tx, frames_rx) = mpsc::channel(QUEUE_DEPTH);
        let (incoming_tx, incoming_rx) = mpsc::channel(QUEUE_DEPTH);
        let connections = Arc::new(AtomicUsize::new(0));
        let dropped_frames = Arc::new(AtomicU64::new(0));

        let listener = match bind_socket(&socket_path, label) {
            Ok(listener) => listener,
            Err(_) => {
                return Self {
                    frames: frames_tx,
                    connections,
                    dropped_frames,
                    label,
                };
            }
        };

        info!(path = %socket_path.display(), "listening for subscribers");

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _addr)) => {
                        // Deliberately not counted here: the writer publishes the count
                        // once the socket is in its own client list, so there is exactly
                        // one writer of `connections`. See `run_writer`.
                        info!("subscriber connected");
                        if incoming_tx.send(stream).await.is_err() {
                            // Writer task is gone, so nothing can serve this client.
                            break;
                        }
                    }
                    Err(e) => {
                        // Accept errors are transient on some platforms (EMFILE, signal
                        // interrupts). Back off and keep listening rather than tearing
                        // down the channel for the whole process lifetime.
                        error!(error = %e, "error accepting connection");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
            debug!("accept loop stopped");
        });

        let writer_connections = Arc::clone(&connections);
        let writer_dropped = Arc::clone(&dropped_frames);
        let writer_path = socket_path.clone();
        tokio::spawn(async move {
            run_writer(
                frames_rx,
                incoming_rx,
                writer_connections,
                writer_dropped,
                framer,
                label,
            )
            .await;
            // The listener lives in the accept task, so the socket file is only
            // removable once the writer is done with it.
            let _ = std::fs::remove_file(&writer_path);
            debug!(path = %writer_path.display(), "writer stopped");
        });

        Self {
            frames: frames_tx,
            connections,
            dropped_frames,
            label,
        }
    }

    /// Whether anyone is listening. Producers use this to skip building a frame at all.
    pub(crate) fn has_subscribers(&self) -> bool {
        self.subscriber_count() > 0
    }

    /// Live subscriber count. Tests assert on the exact number after fan-out.
    pub(crate) fn subscriber_count(&self) -> usize {
        self.connections.load(Ordering::Relaxed)
    }

    /// Queue `frame` for delivery. Never blocks the caller.
    pub(crate) fn push(&self, frame: T) {
        match self.frames.try_send(frame) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                let total = self.dropped_frames.fetch_add(1, Ordering::Relaxed) + 1;
                if total % DROP_LOG_INTERVAL == 1 {
                    warn!(
                        channel = self.label,
                        dropped_total = total,
                        "queue full, dropping frame; subscriber is too slow"
                    );
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                debug!("writer stopped, push disabled");
            }
        }
    }
}

/// Remove a stale socket left by a previous run, then bind.
fn bind_socket(path: &Path, label: &str) -> std::io::Result<UnixListener> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    // A leftover file from an earlier run makes bind() fail with EADDRINUSE.
    if path.exists() {
        std::fs::remove_file(path)?;
    }
    UnixListener::bind(path).inspect_err(|e| {
        error!(path = %path.display(), channel = label, error = %e, "failed to bind socket");
    })
}

/// Single owner of every client connection, so writes are serialized without a lock
/// and one payload is framed once no matter how many subscribers there are.
///
/// Also the only writer of `connections`, in both directions: clients are added when
/// drained off `incoming` and removed after a delivery that found them dead. Counting
/// at accept time instead handed the count a second writer, and the `store` below could
/// then overwrite an increment with zero -- unrecoverably, because producers gate on
/// that count and so stopped queueing the very frames that would re-run this task.
///
/// The cost of publishing from here instead is a short window in which an accepted
/// client is not yet counted and can miss the frame being delivered at that moment.
/// That one is recoverable: a simulator treats a missed push as a cache miss and reads
/// the object from the store, whereas a permanently zero count recovered only when
/// *another* client happened to connect.
async fn run_writer<T: Send + Sync + 'static>(
    mut frames: mpsc::Receiver<T>,
    mut incoming: mpsc::Receiver<UnixStream>,
    connections: Arc<AtomicUsize>,
    dropped_frames: Arc<AtomicU64>,
    framer: impl Fn(&T) -> Option<Vec<u8>> + Send + Sync + 'static,
    label: &'static str,
) {
    let mut clients: Vec<UnixStream> = Vec::new();

    loop {
        tokio::select! {
            biased;

            maybe_stream = incoming.recv() => {
                match maybe_stream {
                    Some(stream) => {
                        clients.push(stream);
                        connections.store(clients.len(), Ordering::Relaxed);
                    }
                    None => break,
                }
            }

            maybe_frame = frames.recv() => {
                let Some(frame) = maybe_frame else {
                    // Producer dropped: shut down and release the socket path.
                    break;
                };
                if clients.is_empty() {
                    // Nothing to deliver. Counted as dropped so an operator can tell
                    // the push path was active but had no audience.
                    dropped_frames.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                deliver(&frame, &mut clients, &framer, label).await;
                connections.store(clients.len(), Ordering::Relaxed);
            }
        }
    }
}

/// Write one frame to every client, dropping those that fail or stall. Dead clients are
/// removed from `clients` in place; the caller republishes the subscriber count from it,
/// so this task stays the only writer of that count.
async fn deliver<T>(
    frame: &T,
    clients: &mut Vec<UnixStream>,
    framer: &(impl Fn(&T) -> Option<Vec<u8>> + ?Sized),
    label: &str,
) {
    let Some(frame_bytes) = framer(frame) else {
        // Not a per-client problem; skip the frame for everyone.
        error!(channel = label, "failed to frame payload, skipping frame");
        return;
    };

    let mut retained = Vec::with_capacity(clients.len());
    for mut client in clients.drain(..) {
        let written = tokio::time::timeout(WRITE_TIMEOUT, async {
            client.write_all(&frame_bytes).await
        })
        .await;
        match written {
            Ok(Ok(())) => retained.push(client),
            Ok(Err(e)) => {
                info!(channel = label, error = %e, "dropping subscriber, write failed");
            }
            Err(_) => {
                warn!(
                    channel = label,
                    timeout_ms = WRITE_TIMEOUT.as_millis(),
                    "dropping subscriber, write timed out"
                );
            }
        }
    }
    *clients = retained;
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    fn scratch_socket(label: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "sui_mev_socket_test_{}_{}_{}.sock",
            std::process::id(),
            label,
            n
        ))
    }

    /// Larger than any platform's Unix socket buffer, so a client that never reads it
    /// parks the writer inside `deliver` on a blocked `write_all`.
    const STALL_PAYLOAD_BYTES: usize = 16 << 20;

    /// Poll until at least `expected` subscribers are counted, for the cases where the
    /// count only ever moves upwards.
    async fn await_subscribers(fanout: &SocketFanOut<Vec<u8>>, expected: usize) {
        for _ in 0..1000 {
            if fanout.subscriber_count() >= expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("subscriber never registered on the fan-out");
    }

    /// The count once it stops moving, so a value that is merely in flight cannot pass
    /// as the outcome.
    ///
    /// Needed because the buggy interleaving publishes a *correct-looking* count first:
    /// accept increments it, and only later does the delivery overwrite it. Waiting for
    /// quiet is also what keeps this honest on platforms where the big frame does not
    /// park the writer at all, where the two tasks settle without ever colliding.
    ///
    /// The window must exceed `WRITE_TIMEOUT`: an eviction only publishes a new count
    /// once the write deadline fires.
    async fn settled_subscriber_count(fanout: &SocketFanOut<Vec<u8>>) -> usize {
        const SETTLE_POLLS: u32 = 200; // 2s without a change
        let mut last = fanout.subscriber_count();
        let mut unchanged = 0;
        for _ in 0..2000 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let now = fanout.subscriber_count();
            if now == last {
                unchanged += 1;
                if unchanged >= SETTLE_POLLS {
                    return now;
                }
            } else {
                unchanged = 0;
                last = now;
            }
        }
        last
    }

    /// A client that connects while a delivery is in flight must still be counted.
    ///
    /// Regression for the lost update: the accept task used to increment the count while
    /// the writer task overwrote it with `store`, so an increment landing during a
    /// delivery that dropped every client was erased. Producers skip work when
    /// `has_subscribers()` is false, so the erased subscriber then never received a
    /// frame -- and never would, because the zero count also stopped the writer from
    /// running again. The symptom is a socket that is bound, connected, and permanently
    /// silent, with no error on either side.
    #[tokio::test]
    async fn a_subscriber_that_connects_mid_delivery_is_still_counted() {
        let path = scratch_socket("midconn");
        let fanout = SocketFanOut::<Vec<u8>>::new(path.clone(), "test channel", |payload| {
            Some(payload.clone())
        });

        let stalled = UnixStream::connect(&path)
            .await
            .expect("connect stalled subscriber");
        await_subscribers(&fanout, 1).await;

        // Park the writer: this frame can only be partially absorbed by the socket
        // buffer of a subscriber that never reads.
        fanout.push(vec![0u8; STALL_PAYLOAD_BYTES]);
        tokio::time::sleep(Duration::from_millis(200)).await;

        // The reconnect that used to be counted and then erased.
        let mut late = UnixStream::connect(&path)
            .await
            .expect("connect late subscriber");
        // Let the accept task observe it before failing the in-flight delivery.
        tokio::time::sleep(Duration::from_millis(200)).await;
        drop(stalled);

        // Judge the count only after it stops moving, and before queueing anything else:
        // pushing another frame would hand the writer a delivery that republishes the
        // count, hiding the very bug under test. The producers cannot do that, which is
        // why the old zero was permanent.
        assert_eq!(
            settled_subscriber_count(&fanout).await,
            1,
            "the subscriber that arrived during a delivery must end up counted"
        );

        // And the channel must actually be live for it, not merely counted.
        fanout.push(b"after".to_vec());
        let mut buf = [0u8; 5];
        tokio::time::timeout(Duration::from_secs(10), late.read_exact(&mut buf))
            .await
            .expect("late subscriber receives a later frame")
            .expect("read frame");
        assert_eq!(&buf, b"after");

        drop(fanout);
        let _ = std::fs::remove_file(&path);
    }
}
