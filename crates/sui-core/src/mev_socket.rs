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

        let accept_connections = Arc::clone(&connections);
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _addr)) => {
                        accept_connections.fetch_add(1, Ordering::Relaxed);
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
                    Some(stream) => clients.push(stream),
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
                if deliver(&frame, &mut clients, &framer, label).await == 0 {
                    connections.store(0, Ordering::Relaxed);
                    continue;
                }
                connections.store(clients.len(), Ordering::Relaxed);
            }
        }
    }
}

/// Write one frame to every client, dropping those that fail or stall. Returns how
/// many clients are still live.
async fn deliver<T>(
    frame: &T,
    clients: &mut Vec<UnixStream>,
    framer: &(impl Fn(&T) -> Option<Vec<u8>> + ?Sized),
    label: &str,
) -> usize {
    let Some(frame_bytes) = framer(frame) else {
        // Not a per-client problem; skip the frame for everyone.
        error!(channel = label, "failed to frame payload, skipping frame");
        return clients.len();
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
    let live = retained.len();
    *clients = retained;
    live
}
