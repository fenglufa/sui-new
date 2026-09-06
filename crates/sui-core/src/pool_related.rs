// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! MEV patch (block B): pool-related object self-learning.
//!
//! AMM pool objects are shared and hot, so they are constantly being evicted from and
//! re-read out of the execution cache. This module records the object ids that produce
//! an execution-cache miss, which is a good proxy for "somebody keeps reading this
//! object". The resulting id set is later used by the push channel (block A) to decide
//! which committed writes are worth notifying an MEV client about, instead of
//! broadcasting every transaction in a checkpoint.
//!
//! The set is loaded from a file at startup and appended to as new ids are learned, so
//! it survives a node restart. Learning is off unless `SUI_RECORD_POOL_RELATED_IDS` is
//! set; reading the file always happens, so a node can consume a list produced by an
//! earlier run without recording anything itself.
//!
//! This is a patch-owned module: nothing here is upstream, and upstream files are only
//! touched at the single call site in `execution_cache::writeback_cache`.

use dashmap::DashSet;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex as StdMutex;
use std::sync::OnceLock;
use sui_types::base_types::ObjectID;
use tracing::{debug, warn};

/// Explicit path to the id file. Overrides [`DEFAULT_SUBPATH`].
const PATH_ENV: &str = "SUI_POOL_RELATED_IDS_PATH";
/// When set (to anything other than a falsy value), newly observed cache-miss ids are
/// appended to the id file.
const RECORD_ENV: &str = "SUI_RECORD_POOL_RELATED_IDS";
/// Fallback location, relative to `$HOME`, matching the layout the patch used on the
/// operator's node (`/home/ubuntu/sui/pool_related_ids.txt`).
const DEFAULT_SUBPATH: &str = "sui/pool_related_ids.txt";

/// Synthetic ids used by tests and mock transactions. They miss the cache on every
/// run and carry no meaning, so they are never logged. Parsed once and compared by
/// value: this runs on the cache-miss hot path, where formatting an id to a string per
/// miss would allocate even when debug logging is off.
static UNINTERESTING_IDS: OnceLock<[ObjectID; 2]> = OnceLock::new();

fn uninteresting_ids() -> &'static [ObjectID; 2] {
    UNINTERESTING_IDS.get_or_init(|| {
        [
            "0x0000000000000000000000000000000000000000000000000000000000001337"
                .parse()
                .expect("valid object id"),
            "0x0000000000000000000000000000000000000000000000000000000000001338"
                .parse()
                .expect("valid object id"),
        ]
    })
}

struct PoolRelatedState {
    related_ids: DashSet<ObjectID>,
    /// Opened lazily on the first id we actually learn, and only if recording is enabled.
    append: StdMutex<Option<File>>,
}

static POOL_RELATED_STATE: OnceLock<PoolRelatedState> = OnceLock::new();
static RECORDING: OnceLock<bool> = OnceLock::new();
static ID_FILE_PATH: OnceLock<PathBuf> = OnceLock::new();

/// Parse the persisted id file: one [`ObjectID`] per line.
///
/// Blank lines are skipped and unparseable lines are counted rather than fatal, so a
/// corrupted file degrades to a partially populated set instead of stopping the node.
/// Returns the ids and the number of lines that were ignored.
fn parse_ids_from_content(content: &str) -> (DashSet<ObjectID>, usize) {
    let related_ids = DashSet::new();
    let mut malformed = 0usize;

    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        match line.parse::<ObjectID>() {
            Ok(id) => {
                related_ids.insert(id);
            }
            Err(_) => malformed += 1,
        }
    }

    (related_ids, malformed)
}

/// Whether newly observed cache-miss ids should be persisted.
///
/// The flag is opt-in: unset or any falsy spelling means "read the file, learn nothing".
fn parse_recording_flag(raw: Option<&str>) -> bool {
    match raw {
        Some(value) => !matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "" | "0" | "false" | "no" | "off"
        ),
        None => false,
    }
}

impl PoolRelatedState {
    /// Read the persisted id set. A missing or unreadable file is not fatal: the node
    /// starts with an empty set and learns from scratch.
    fn load(path: &Path) -> Self {
        let related_ids = match std::fs::read_to_string(path) {
            Ok(content) => {
                let (related_ids, malformed) = parse_ids_from_content(&content);
                if malformed > 0 {
                    warn!(
                        malformed,
                        path = %path.display(),
                        "pool_related: ignored unparseable ids in file"
                    );
                }
                debug!(
                    count = related_ids.len(),
                    path = %path.display(),
                    "pool_related: loaded known pool object ids"
                );
                related_ids
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                warn!(
                    path = %path.display(),
                    "pool_related: id file not found, starting with an empty set"
                );
                DashSet::new()
            }
            Err(err) => {
                warn!(
                    error = %err,
                    path = %path.display(),
                    "pool_related: failed to read id file, starting with an empty set"
                );
                DashSet::new()
            }
        };

        Self {
            related_ids,
            append: StdMutex::new(None),
        }
    }

    fn record(&self, path: &Path, object_id: &ObjectID) {
        if self.related_ids.contains(object_id) {
            return;
        }
        self.related_ids.insert(*object_id);

        let Ok(mut guard) = self.append.lock() else {
            return;
        };
        if guard.is_none() {
            *guard = open_append(path);
        }
        if let Some(file) = guard.as_mut() {
            // `File` is unbuffered, so this is a single append syscall. It only happens
            // once per newly learned id, never on a repeat.
            let _ = writeln!(file, "{object_id}");
        }
    }
}

fn open_append(path: &Path) -> Option<File> {
    if let Some(parent) = path.parent()
        && let Err(err) = std::fs::create_dir_all(parent)
    {
        warn!(error = %err, path = %path.display(), "pool_related: failed to create parent dir");
        return None;
    }

    match OpenOptions::new().create(true).append(true).open(path) {
        Ok(file) => Some(file),
        Err(err) => {
            warn!(error = %err, path = %path.display(), "pool_related: failed to open id file for append");
            None
        }
    }
}

fn id_file_path() -> &'static Path {
    ID_FILE_PATH
        .get_or_init(|| {
            if let Ok(path) = std::env::var(PATH_ENV) {
                return PathBuf::from(path);
            }
            let home = std::env::var("HOME")
                .unwrap_or_else(|_| std::env::temp_dir().display().to_string());
            PathBuf::from(home).join(DEFAULT_SUBPATH)
        })
        .as_path()
}

fn recording_enabled() -> bool {
    *RECORDING.get_or_init(|| parse_recording_flag(std::env::var(RECORD_ENV).ok().as_deref()))
}

fn state() -> &'static PoolRelatedState {
    POOL_RELATED_STATE.get_or_init(|| PoolRelatedState::load(id_file_path()))
}

/// Every object id known to be pool-related: the persisted set plus whatever this
/// process has learned since startup.
// Consumed by block A (push channel), which filters committed writes against this set.
#[allow(dead_code)]
pub(crate) fn related_ids() -> &'static DashSet<ObjectID> {
    &state().related_ids
}

/// Called on every execution-cache miss that has a concrete object id.
///
/// Logs the miss, and appends the id to the persisted set when recording is enabled.
pub(crate) fn on_cache_miss(table: &'static str, level: &'static str, object_id: &ObjectID) {
    if !uninteresting_ids().contains(object_id) {
        debug!(
            target: "cache_metrics",
            table,
            level,
            %object_id,
            "Cache miss"
        );
    }

    if recording_enabled() {
        state().record(id_file_path(), object_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// Unique scratch path under the system temp dir. `tempfile` is not a dependency of
    /// this crate, and adding one purely for these tests would widen the patch's footprint
    /// inside an upstream `Cargo.toml`.
    fn scratch_path(label: &str) -> PathBuf {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "sui_pool_related_test_{}_{}_{}",
            std::process::id(),
            label,
            n
        ))
    }

    fn id(n: u8) -> ObjectID {
        format!("0x{n:0>64x}").parse().expect("valid object id")
    }

    #[test]
    fn parses_ids_and_ignores_blank_and_malformed_lines() {
        let (a, b) = (id(1), id(2));
        let content = format!("{a}\n\n  {b}  \nnot-an-id\n{a}\n");

        let (set, malformed) = parse_ids_from_content(&content);

        assert_eq!(malformed, 1, "only the one garbage line is counted");
        assert!(set.contains(&a));
        assert!(set.contains(&b));
        assert_eq!(set.len(), 2, "a repeated id collapses");
    }

    #[test]
    fn recording_flag_is_opt_in() {
        assert!(!parse_recording_flag(None), "unset means off");
        for falsy in ["", "0", "false", "FALSE", " no ", "Off"] {
            assert!(
                !parse_recording_flag(Some(falsy)),
                "{falsy:?} should be falsy"
            );
        }
        for truthy in ["1", "true", "yes", "on", "anything"] {
            assert!(
                parse_recording_flag(Some(truthy)),
                "{truthy:?} should be truthy"
            );
        }
    }

    #[test]
    fn missing_file_loads_as_empty_set_instead_of_panicking() {
        let state = PoolRelatedState::load(&scratch_path("missing"));
        assert_eq!(state.related_ids.len(), 0);
    }

    #[test]
    fn learned_ids_persist_and_reload() {
        let path = scratch_path("roundtrip");
        let state = PoolRelatedState::load(&path);
        let (a, b) = (id(10), id(11));

        state.record(&path, &a);
        state.record(&path, &b);
        state.record(&path, &a);

        let written = std::fs::read_to_string(&path).expect("file should have been created");
        assert_eq!(
            written.lines().count(),
            2,
            "one line per newly learned id, repeats must not append:\n{written}"
        );

        let reloaded = PoolRelatedState::load(&path);
        assert!(reloaded.related_ids.contains(&a));
        assert!(reloaded.related_ids.contains(&b));
        assert_eq!(reloaded.related_ids.len(), 2);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn an_unwritable_file_still_learns_in_memory() {
        // Make the intended parent directory path point through an existing regular file,
        // so both the read and the create_dir_all inside open_append fail. Learning must
        // degrade to in-memory instead of taking the node down.
        let blocker = scratch_path("unwritable_blocker");
        std::fs::write(&blocker, b"not a directory").expect("setup");
        let path = blocker.join("nested/pool_related_ids.txt");

        let state = PoolRelatedState::load(&path);
        let a = id(20);
        state.record(&path, &a);

        assert!(state.related_ids.contains(&a));

        let _ = std::fs::remove_file(&blocker);
    }
}
