// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Tests for MEV patch (block D): letting an out-of-process simulator attach to the
//! database a node owns, and keeping its execution cache in step with it.

use super::*;
use crate::authority::authority_store_tables::AuthorityPerpetualTables;
#[cfg(not(tidehunter))]
use crate::authority::authority_store_types::get_store_object;
use std::path::Path;
use sui_swarm_config::network_config_builder::ConfigBuilder;
use sui_types::base_types::{ObjectID, SequenceNumber, SuiAddress};
use sui_types::digests::TransactionDigest;
use sui_types::object::{MoveObject, Object, Owner};
use sui_types::storage::ObjectKey;
use tempfile::tempdir;
#[cfg(not(tidehunter))]
use typed_store::traits::Map;

/// A coin whose parent transaction is the genesis marker, so the store accepts it
/// through `insert_genesis_object` (its one public single-object write path). Ids are
/// random: the store opened below already holds the genesis framework objects, and low
/// fixed ids would collide with them.
fn coin_at(id: ObjectID, version: u64) -> Object {
    Object::new_move(
        MoveObject::new_gas_coin(SequenceNumber::from(version), id, 1_000),
        Owner::AddressOwner(SuiAddress::default()),
        TransactionDigest::genesis_marker(),
    )
}

async fn store_over_dir(dir: &Path) -> Arc<AuthorityStore> {
    let tables = Arc::new(AuthorityPerpetualTables::open(dir, None, None));
    let config = ConfigBuilder::new_with_temp_dir().build();
    AuthorityStore::open_with_committee_for_testing(
        tables,
        config.committee_with_network().committee(),
        &config.genesis,
    )
    .await
    .expect("open store")
}

/// The point of `reload_objects`: a pushed object becomes readable without a disk
/// round trip, which is what makes the push channel worth having at all.
#[tokio::test]
async fn reload_objects_publishes_an_object_into_the_latest_cache() {
    let dir = tempdir().unwrap();
    let store = store_over_dir(dir.path()).await;
    let cache = Arc::new(WritebackCache::new_for_tests(store));

    let id = ObjectID::random();
    let object = coin_at(id, 1);
    let object_ref = object.compute_object_reference();

    assert_eq!(
        cache.get_latest_object_ref_or_tombstone(id),
        None,
        "nothing wrote this object, so it must be absent"
    );

    cache.reload_objects(vec![(id, object)]);

    assert_eq!(
        cache.get_latest_object_ref_or_tombstone(id),
        Some(object_ref),
        "reloaded object should be readable from the cache"
    );
}

/// A reload must not resurrect a stale version. `MonotonicCache` treats an older write
/// over a newer entry as a fatal invariant violation, so `reload_objects` has to filter
/// it out itself: out-of-order arrival is ordinary for a pushed stream.
#[tokio::test]
async fn reload_objects_does_not_downgrade_a_newer_cached_version() {
    let dir = tempdir().unwrap();
    let store = store_over_dir(dir.path()).await;
    let cache = Arc::new(WritebackCache::new_for_tests(store));

    let id = ObjectID::random();
    let older = coin_at(id, 1);
    let newer = coin_at(id, 9);

    cache.reload_objects(vec![(id, newer.clone())]);
    cache.reload_objects(vec![(id, older)]);

    assert_eq!(
        cache.get_latest_object_ref_or_tombstone(id),
        Some(newer.compute_object_reference()),
        "the newest version must win regardless of arrival order"
    );
}

/// The reason block D exists: a second process can attach to a database a node owns,
/// see its committed writes after a catch-up, and still be unable to corrupt it.
/// Secondary instances are a RocksDB feature, so this has no tidehunter equivalent.
#[cfg(not(tidehunter))]
#[tokio::test]
async fn a_secondary_handle_reads_the_primaries_writes_and_rejects_its_own() {
    let dir = tempdir().unwrap();
    let store = store_over_dir(dir.path()).await;

    let object = coin_at(ObjectID::random(), 1);
    store.insert_genesis_object(object.clone()).expect("write");

    let secondary = AuthorityPerpetualTables::open_readonly_as_rw(dir.path());
    secondary
        .try_catch_up_with_primary_all()
        .expect("catch up with primary");

    let key = ObjectKey::from(object.compute_object_reference());
    assert_eq!(
        secondary.objects.get(&key).expect("read"),
        Some(get_store_object(object.clone())),
        "secondary should observe the object the primary committed"
    );

    assert!(
        secondary.objects.remove(&key).is_err(),
        "a secondary instance must not accept writes"
    );

    drop(secondary);
}

/// `reload_objects` parks pushed versions in the dirty set, which nothing would ever
/// flush on a process whose store rejects writes. The catch-up hook is what bounds it:
/// once the store can answer the read, the parked entry has to go.
#[tokio::test]
async fn reloaded_entries_are_dropped_once_the_store_covers_them() {
    let dir = tempdir().unwrap();
    let store = store_over_dir(dir.path()).await;
    let cache = Arc::new(WritebackCache::new_for_tests(store.clone()));

    let id = ObjectID::random();
    let object = coin_at(id, 1);
    let object_ref = object.compute_object_reference();
    cache.reload_objects(vec![(id, object.clone())]);

    drop_reloaded_entries_the_store_covers(&cache);
    assert!(
        cache.dirty.objects.contains_key(&id),
        "the store cannot answer this read yet, so the pushed copy must stay"
    );
    assert_eq!(
        cache.get_latest_object_ref_or_tombstone(id),
        Some(object_ref)
    );

    // What a successful catch-up looks like from here: the store now has the version.
    store.insert_genesis_object(object).expect("write");
    drop_reloaded_entries_the_store_covers(&cache);

    assert!(
        !cache.dirty.objects.contains_key(&id),
        "a version the store can answer must not stay parked in the dirty set"
    );
    assert_eq!(
        cache.get_latest_object_ref_or_tombstone(id),
        Some(object_ref),
        "the read now falls through to the store"
    );
}
