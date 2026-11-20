//! TAG-2753: an index checkpoint deletes the objects that a stop left after a retention or
//! hard-delete removal, and only those.

use super::*;
use object_store::PutPayload;
use object_store::local::LocalFileSystem;

/// The checkpoint loop's state as the service starts it: from the index loaded at startup.
fn checkpoint_state(h: &CompactionHarness) -> IndexCheckpointState {
    IndexCheckpointState::new(
        h.write_buffer
            .take_index_checkpoint_base()
            .expect("the startup index was already taken"),
    )
}

/// Two files, one past its retention period. The next flush's snapshot records the expired
/// file's removal, then the flush deletes its object; putting the object back leaves the store
/// as a stop between the two would. Returns the expired file's path and the live file.
async fn leave_an_object_after_a_retention_removal(
    h: &CompactionHarness,
) -> (ObjPath, ParquetFile) {
    h.write_gen1("testtable value=1 10000000000").await;
    h.write_gen1("testtable value=2 7200000000000").await;
    let expired = h.indexed_file_at(10_000_000_000);
    let live = h.indexed_file_at(7_200_000_000_000);
    let expired_path = ObjPath::from(expired.path.as_str());
    let bytes = h
        .object_store
        .get(&expired_path)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    h.catalog
        .set_retention_period_for_database(CompactionHarness::DB, Duration::from_secs(3_600))
        .await
        .unwrap();
    h.set_now(Duration::from_secs(7_300));

    h.write_buffer
        .write_lp(
            data_types::NamespaceName::new(CompactionHarness::DB).unwrap(),
            "testtable value=3 7250000000000",
            iox_time::Time::from_timestamp_nanos(0),
            false,
            crate::Precision::Nanosecond,
            false,
        )
        .await
        .unwrap();
    {
        // The permit is the WAL's snapshot permit, which a checkpoint waits for.
        let (snapshot_done, _, _permit) = h
            .write_buffer
            .wal()
            .force_flush_buffer()
            .await
            .expect("the flush snapshots");
        snapshot_done.await.expect("snapshot failed");
    }
    assert!(
        !h.indexed_files().contains(&expired),
        "retention removed it"
    );
    tokio::time::timeout(Duration::from_secs(10), async {
        while h.object_store.head(&expired_path).await.is_ok() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the flush deletes the object");
    h.object_store
        .put(&expired_path, bytes.into())
        .await
        .unwrap();
    (expired_path, live)
}

/// A stop after a flush's snapshot records a retention removal, but before the removed file's
/// object is deleted, leaves an object that no index entry points to. Startup deletes such
/// objects, but only in the snapshots it loads, and a checkpoint replaces the snapshots before
/// it. So the checkpoint deletes them before it is written (TAG-2753).
#[tokio::test]
async fn test_checkpoint_deletes_objects_a_stop_left_after_a_retention_removal() {
    let h = CompactionHarness::new().await;
    let (expired_path, live) = leave_an_object_after_a_retention_removal(&h).await;

    let mut state = checkpoint_state(&h);
    h.service.write_index_checkpoint(&mut state).await.unwrap();
    assert!(
        matches!(
            h.object_store.head(&expired_path).await,
            Err(object_store::Error::NotFound { .. })
        ),
        "the checkpoint deletes the object before it is written"
    );
    assert!(
        h.object_store
            .head(&ObjPath::from(live.path.as_str()))
            .await
            .is_ok(),
        "the live file's object stays"
    );
}

/// The first checkpoint after a restart replaces the snapshots that start-up loaded, so it
/// starts with their marked removals: a second stop before start-up's own deletes finish still
/// leaves nothing behind once that checkpoint is written.
#[tokio::test]
async fn test_first_checkpoint_after_a_restart_deletes_the_removals_startup_loaded() {
    let h = CompactionHarness::new().await;
    let (expired_path, _) = leave_an_object_after_a_retention_removal(&h).await;

    let (_, restarted) = h.restart_loading_all().await;
    let state = IndexCheckpointState::new(
        restarted
            .take_index_checkpoint_base()
            .expect("the startup index"),
    );
    assert_eq!(state.removals.unindexed(&state.fold), vec![expired_path]);
}

/// A delete that fails at a checkpoint is kept for the next one: once a checkpoint replaces the
/// snapshot that recorded the removal, nothing else would find the object.
#[tokio::test]
async fn test_a_delete_that_fails_at_a_checkpoint_is_tried_again_at_the_next() {
    let dir = test_helpers::tmp_dir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let h = CompactionHarness::with_object_store(store).await;
    let (expired_path, _) = leave_an_object_after_a_retention_removal(&h).await;
    // A directory where the object was: deleting it fails, and not with NotFound.
    let on_disk = dir.path().join(expired_path.as_ref());
    std::fs::remove_file(&on_disk).unwrap();
    std::fs::create_dir(&on_disk).unwrap();

    let mut state = checkpoint_state(&h);
    h.service.write_index_checkpoint(&mut state).await.unwrap();
    assert!(on_disk.is_dir(), "the first checkpoint's delete failed");

    std::fs::remove_dir(&on_disk).unwrap();
    h.object_store
        .put(&expired_path, PutPayload::from_static(b"orphan"))
        .await
        .unwrap();
    h.service.write_index_checkpoint(&mut state).await.unwrap();
    assert!(
        matches!(
            h.object_store.head(&expired_path).await,
            Err(object_store::Error::NotFound { .. })
        ),
        "the next checkpoint deletes the object"
    );
}

/// The index check's removals are not marked: it keeps the objects of the entries it prunes,
/// and undoing a wrong prune needs them. So a checkpoint after a prune leaves the object of a
/// pruned entry in object storage, though no index entry points to it.
#[tokio::test]
async fn test_checkpoint_keeps_the_object_of_an_entry_the_index_check_pruned() {
    let h = CompactionHarness::new().await;
    h.write_gen1("testtable value=1 10000000000").await;
    h.write_gen1("testtable value=2 7200000000000").await;
    let stale = h.indexed_file_at(10_000_000_000);
    let stale_path = ObjPath::from(stale.path.as_str());
    // Other bytes at the entry's path: the object's size no longer matches the entry.
    h.object_store
        .put(&stale_path, PutPayload::from_static(b"overwritten"))
        .await
        .unwrap();
    h.service.check_index_for_missing_files().await;
    assert!(
        !h.indexed_files().contains(&stale),
        "the index check pruned the entry"
    );

    let mut state = checkpoint_state(&h);
    h.service.write_index_checkpoint(&mut state).await.unwrap();
    assert!(
        h.object_store.head(&stale_path).await.is_ok(),
        "the checkpoint keeps the pruned entry's object"
    );
}
