//! TAG-2497: index checkpoints. Startup loads the newest checkpoint and every snapshot after it,
//! so a checkpoint and the snapshots after it must rebuild exactly the index that loading every
//! snapshot builds, whatever was written around it and while it was being written.

use super::*;
use crate::paths::SnapshotInfoFilePath;
use crate::write_buffer::persisted_files::SnapshotFold;
use futures::TryStreamExt;
use influxdb3_test_helpers::object_store::{RequestCountedObjectStore, SynchronizedObjectStore};
use tokio::sync::Notify;

type Index = BTreeMap<(DbId, TableId), Vec<ParquetFile>>;

/// The checkpoint loop's state as the service starts it: from the index loaded at startup.
fn checkpoint_state(h: &CompactionHarness) -> IndexCheckpointState {
    IndexCheckpointState::new(
        h.write_buffer
            .take_index_checkpoint_base()
            .expect("the startup index was already taken"),
    )
}

fn sorted(mut index: Index) -> Index {
    for files in index.values_mut() {
        files.sort_by(|a, b| a.path.cmp(&b.path));
    }
    index
}

/// Every entry of a write buffer's index, by table, sorted by path.
fn index_of(catalog: &Catalog, write_buffer: &WriteBufferImpl) -> Index {
    let mut index = Index::new();
    for db in catalog.list_db_schema() {
        for table in db.tables() {
            let files = write_buffer
                .persisted_files()
                .get_files(db.id, table.table_id);
            if !files.is_empty() {
                index.insert((db.id, table.table_id), files);
            }
        }
    }
    sorted(index)
}

async fn snapshot_paths(h: &CompactionHarness) -> Vec<ObjPath> {
    let mut paths: Vec<ObjPath> = h
        .object_store
        .list(Some(&SnapshotInfoFilePath::dir("test-host")))
        .map_ok(|meta| meta.location)
        .try_collect()
        .await
        .unwrap();
    // Newest first, as startup sorts them.
    paths.sort();
    paths
}

/// The index that loading every snapshot info file (and no checkpoint) builds: what a
/// checkpoint and the snapshots after it must reproduce.
async fn index_from_every_snapshot(h: &CompactionHarness) -> Index {
    let mut fold = SnapshotFold::default();
    for path in snapshot_paths(h).await.into_iter().rev() {
        if SnapshotInfoFilePath::is_checkpoint(&path) {
            continue;
        }
        let bytes = h
            .object_store
            .get(&path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let PersistedSnapshotVersion::V1(snapshot) = serde_json::from_slice(&bytes).unwrap();
        fold.apply(snapshot);
    }
    sorted(fold.into_files())
}

/// The sequence numbers of the index checkpoints in object storage, newest first.
async fn checkpoints_in_store(h: &CompactionHarness) -> Vec<SnapshotSequenceNumber> {
    snapshot_paths(h)
        .await
        .iter()
        .filter(|p| SnapshotInfoFilePath::is_checkpoint(p))
        .map(|p| SnapshotInfoFilePath::sequence_number(p).unwrap())
        .collect()
}

fn test_table(h: &CompactionHarness) -> (DbId, TableId) {
    let db = h.catalog.db_schema(CompactionHarness::DB).unwrap();
    let table_id = db
        .table_definition(CompactionHarness::TABLE)
        .unwrap()
        .table_id;
    (db.id, table_id)
}

/// The history the equivalence test writes, one step at a time: data snapshots, compaction
/// snapshots (outputs added, inputs removed), a removal-only prune snapshot, and gen1 files that
/// never compact.
const STEPS: usize = 6;

async fn run_step(h: &CompactionHarness, step: usize) {
    match step {
        // Window 0: one gen1 file, below the 2 a job needs.
        0 => h.write_gen1("testtable value=0 10000000000").await,
        1 => {
            compact_tag_2497_window(h, 1).await;
        }
        // The lone gen1 file's object goes missing, and the index check prunes it.
        2 => {
            let lone = h.indexed_file_at(10_000_000_000);
            h.delete_object(&lone).await;
            let pruned = h
                .service
                .prune_index_entries(BTreeMap::from([(test_table(h), vec![lone])]))
                .await;
            assert_eq!(pruned.count(), 1, "test setup: the prune removed nothing");
        }
        3 => {
            compact_tag_2497_window(h, 2).await;
        }
        // Window 3: one gen1 file that stays.
        4 => h.write_gen1("testtable value=3 370000000000").await,
        5 => {
            compact_tag_2497_window(h, 4).await;
        }
        _ => unreachable!(),
    }
}

/// A checkpoint written at any point of a history of data snapshots, compactions and a prune,
/// followed by a second one that starts from it, plus the snapshots after them, rebuilds
/// exactly the index the running server has, which is the index that loading every snapshot
/// builds.
#[tokio::test]
async fn test_checkpoint_and_the_snapshots_after_it_rebuild_the_index_of_every_snapshot() {
    for checkpoint_at in 0..=STEPS {
        let h = CompactionHarness::with_config(tag_2497_config()).await;
        let mut state = checkpoint_state(&h);
        let mut written = Vec::new();
        for step in 0..=STEPS {
            if step == checkpoint_at || step == checkpoint_at + 2 {
                written.push(h.service.write_index_checkpoint(&mut state).await.unwrap());
            }
            if step < STEPS {
                run_step(&h, step).await;
            }
        }
        assert_eq!(checkpoints_in_store(&h).await.len(), written.len());

        let running = index_of(&h.catalog, &h.write_buffer);
        assert_eq!(
            index_from_every_snapshot(&h).await,
            running,
            "test setup: loading every snapshot should rebuild the running index"
        );
        let (catalog, restarted) = h.restart().await;
        assert_eq!(
            index_of(&catalog, &restarted),
            running,
            "checkpoints written before steps {written:?} (checkpoint_at {checkpoint_at})"
        );
    }
}

/// A restart reads the newest checkpoint and the snapshots after it, and nothing older.
#[tokio::test]
async fn test_restart_after_a_checkpoint_reads_only_it_and_the_snapshots_after_it() {
    let store = Arc::new(RequestCountedObjectStore::new(Arc::new(InMemory::new())));
    let h = CompactionHarness::build(Arc::clone(&store) as _, tag_2497_config()).await;
    h.write_gen1("testtable value=0 10000000000").await;
    for window in 1..=2 {
        compact_tag_2497_window(&h, window).await;
    }
    let mut state = checkpoint_state(&h);
    let checkpoint = h.service.write_index_checkpoint(&mut state).await.unwrap();
    compact_tag_2497_window(&h, 3).await;

    let paths = snapshot_paths(&h).await;
    let reads = |paths: &[ObjPath]| -> Vec<usize> {
        paths
            .iter()
            .map(|p| store.total_read_request_count(p))
            .collect()
    };
    let before = reads(&paths);
    let (catalog, restarted) = h.restart().await;
    let after = reads(&paths);

    let mut read_once = 0;
    for ((path, before), after) in paths.iter().zip(before).zip(after) {
        let seq = SnapshotInfoFilePath::sequence_number(path).unwrap();
        let expected = usize::from(seq >= checkpoint);
        assert_eq!(after - before, expected, "reads of {path} at restart");
        read_once += expected;
    }
    assert_eq!(
        read_once,
        1 + 3,
        "the checkpoint and window 3's 3 snapshots"
    );
    assert_eq!(
        index_of(&catalog, &restarted),
        index_of(&h.catalog, &h.write_buffer)
    );
}

/// The index check's prune (a removal-only snapshot) releases the WAL's snapshot permit before
/// its upload. A checkpoint reserved while that upload is still in flight would fold past the
/// prune without seeing it, and a restart from the checkpoint would load the pruned entry
/// again. The checkpoint must wait for the upload.
#[tokio::test]
async fn test_checkpoint_waits_for_a_prune_snapshot_that_is_still_uploading() {
    // Data snapshots 1 and 2, then the prune's snapshot 3, which is held.
    let held = SnapshotInfoFilePath::new("test-host", SnapshotSequenceNumber::new(3));
    let reached = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let store = SynchronizedObjectStore::new(Arc::new(InMemory::new())).with_put_notifies(
        held.as_ref().clone(),
        Arc::clone(&release),
        Arc::clone(&reached),
    );
    let h = CompactionHarness::build(Arc::new(store), tag_2497_config()).await;
    h.write_gen1("testtable value=1 10000000000").await;
    h.write_gen1("testtable value=2 20000000000").await;
    let stale = h.indexed_file_at(10_000_000_000);
    h.delete_object(&stale).await;
    let mut state = checkpoint_state(&h);

    let prune = h
        .service
        .prune_index_entries(BTreeMap::from([(test_table(&h), vec![stale.clone()])]));
    tokio::pin!(prune);
    tokio::select! {
        _ = &mut prune => panic!("test setup: the prune snapshot was not held"),
        _ = reached.notified() => {}
    }

    let checkpoint = h.service.write_index_checkpoint(&mut state);
    tokio::pin!(checkpoint);
    let written_during_upload = tokio::time::timeout(Duration::from_millis(300), &mut checkpoint)
        .await
        .is_ok();
    release.notify_one();
    assert_eq!(prune.await.count(), 1);
    if !written_during_upload {
        checkpoint.await.unwrap();
    }

    let running = index_of(&h.catalog, &h.write_buffer);
    assert!(!running[&test_table(&h)].contains(&stale));
    let (catalog, restarted) = h.restart().await;
    assert_eq!(
        index_of(&catalog, &restarted),
        running,
        "a restart from the checkpoint must keep the entry pruned"
    );
    assert!(
        !written_during_upload,
        "the checkpoint was written while the prune's snapshot was still uploading"
    );
}

/// The same for a compaction snapshot: a checkpoint that folds past one still uploading would
/// restart with the job's inputs (whose objects the job deletes) and without its output.
#[tokio::test]
async fn test_checkpoint_waits_for_a_compaction_snapshot_that_is_still_uploading() {
    // Data snapshots 1 and 2 (window 1's two gen1 files), then the compaction's snapshot 3.
    let held = SnapshotInfoFilePath::new("test-host", SnapshotSequenceNumber::new(3));
    let reached = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let store = SynchronizedObjectStore::new(Arc::new(InMemory::new())).with_put_notifies(
        held.as_ref().clone(),
        Arc::clone(&release),
        Arc::clone(&reached),
    );
    let h = CompactionHarness::build(Arc::new(store), tag_2497_config()).await;
    h.write_gen1("testtable value=1 130000000000").await;
    h.write_gen1("testtable value=2 190000000000").await;
    h.set_now(Duration::from_secs(240));
    let mut state = checkpoint_state(&h);

    let compaction = h.compact();
    tokio::pin!(compaction);
    tokio::select! {
        _ = &mut compaction => panic!("test setup: the compaction snapshot was not held"),
        _ = reached.notified() => {}
    }

    let checkpoint = h.service.write_index_checkpoint(&mut state);
    tokio::pin!(checkpoint);
    let written_during_upload = tokio::time::timeout(Duration::from_millis(300), &mut checkpoint)
        .await
        .is_ok();
    release.notify_one();
    assert_eq!(compaction.await.len(), 1);
    if !written_during_upload {
        checkpoint.await.unwrap();
    }

    let running = index_of(&h.catalog, &h.write_buffer);
    assert!(running[&test_table(&h)][0].path.contains("/gen2/"));
    let (catalog, restarted) = h.restart().await;
    assert_eq!(
        index_of(&catalog, &restarted),
        running,
        "a restart from the checkpoint must have the output and not the inputs"
    );
    assert!(
        !written_during_upload,
        "the checkpoint was written while the compaction's snapshot was still uploading"
    );
    h.assert_index_readable(2).await;
}

/// Rolling back to an image that predates checkpoints: it loads the newest n snapshots by name
/// and folds them, so a checkpoint among them is an ordinary snapshot that re-adds every file.
/// With the checkpoint in its window it rebuilds the whole index, including the files whose own
/// snapshots are outside the window.
#[tokio::test]
async fn test_the_loader_before_checkpoints_rebuilds_the_index_from_a_window_holding_one() {
    let h = CompactionHarness::with_config(tag_2497_config()).await;
    h.write_gen1("testtable value=0 10000000000").await;
    for window in 1..=3 {
        compact_tag_2497_window(&h, window).await;
    }
    let mut state = checkpoint_state(&h);
    h.service.write_index_checkpoint(&mut state).await.unwrap();
    // Window 4's inputs are added and removed after the checkpoint.
    compact_tag_2497_window(&h, 4).await;
    let running = index_of(&h.catalog, &h.write_buffer);

    // The deployed loader with a window of the checkpoint and the 3 snapshots after it.
    let snapshots = h
        .persister
        .load_snapshots(4)
        .await
        .unwrap()
        .into_iter()
        .map(|PersistedSnapshotVersion::V1(s)| s)
        .collect::<Vec<_>>();
    assert!(snapshots.last().unwrap().checkpoint);
    let files = PersistedFiles::new_from_persisted_snapshots(snapshots);
    let (db, table) = test_table(&h);
    let mut windowed = files.get_files(db, table);
    windowed.sort_by(|a, b| a.path.cmp(&b.path));
    assert_eq!(windowed, running[&(db, table)]);
    assert_eq!(windowed.len(), 5, "the lone gen1 file and 4 gen2 outputs");
}

/// A checkpoint persists no WAL data: when it is the newest snapshot, startup must replay the
/// WAL after the last data snapshot, not after the newest WAL file, and must hand out file ids
/// above every indexed one.
#[tokio::test]
async fn test_checkpoint_records_the_last_persisted_wal_file_and_the_next_file_id() {
    let h = CompactionHarness::new().await;
    h.write_gen1("testtable value=1 10000000000\ntesttable value=2 130000000000")
        .await;
    let persisted_wal = h.newest_snapshot().await.wal_file_sequence_number;
    h.write_wal_only("testtable value=3 140000000000").await;
    assert!(
        h.write_buffer.wal().last_wal_sequence_number().await > persisted_wal,
        "test setup: the WAL must hold a file no snapshot persisted"
    );

    let mut state = checkpoint_state(&h);
    h.service.write_index_checkpoint(&mut state).await.unwrap();
    let newest = h.newest_snapshot().await;
    assert!(
        newest.checkpoint,
        "test setup: the checkpoint must be the newest snapshot"
    );
    assert_eq!(newest.wal_file_sequence_number, persisted_wal);
    let max_id = h
        .indexed_files()
        .iter()
        .map(|f| f.id.as_u64())
        .max()
        .unwrap();
    assert!(newest.next_file_id.as_u64() > max_id);

    // A restart replays value=3 from the WAL; persist it to count it with the rest.
    let (catalog, restarted) = h.restart().await;
    if let Some((snapshot_done, _, _permit)) = restarted.wal().force_flush_buffer().await {
        snapshot_done.await.expect("snapshot failed");
    }
    let db = catalog.db_schema(CompactionHarness::DB).unwrap();
    let table_id = db
        .table_definition(CompactionHarness::TABLE)
        .unwrap()
        .table_id;
    let mut rows = 0;
    for _ in 0..100 {
        rows = restarted
            .persisted_files()
            .get_files(db.id, table_id)
            .iter()
            .map(|f| f.row_count)
            .sum::<u64>();
        if rows >= 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(rows, 3, "2 persisted rows + 1 replayed from the WAL");
}

/// A data snapshot holds the WAL's snapshot permit until its snapshot file is written, and the
/// checkpoint takes that permit after reserving its number: it waits for the data snapshot and
/// includes its files.
#[tokio::test]
async fn test_checkpoint_waits_for_a_data_snapshot_in_progress() {
    let held = SnapshotInfoFilePath::new("test-host", SnapshotSequenceNumber::new(2));
    let reached = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let store = SynchronizedObjectStore::new(Arc::new(InMemory::new())).with_put_notifies(
        held.as_ref().clone(),
        Arc::clone(&release),
        Arc::clone(&reached),
    );
    let h = CompactionHarness::build(Arc::new(store), tag_2497_config()).await;
    h.write_gen1("testtable value=1 10000000000").await;
    let mut state = checkpoint_state(&h);

    // Data snapshot 2 persists value=2 and is held at its snapshot file. Like the WAL's flush
    // task, hold its permit until the snapshot is done.
    h.write_wal_only("testtable value=2 20000000000").await;
    let wal = h.write_buffer.wal();
    let (snapshot_done, data_snapshot, permit) = wal
        .force_flush_buffer()
        .await
        .expect("test setup: no data snapshot");
    assert_eq!(
        data_snapshot.snapshot_sequence_number,
        SnapshotSequenceNumber::new(2)
    );
    let snapshot_task = tokio::spawn({
        let wal = Arc::clone(&wal);
        async move {
            snapshot_done.await.expect("snapshot failed");
            wal.cleanup_snapshot(data_snapshot, permit).await;
        }
    });
    reached.notified().await;

    let checkpoint = h.service.write_index_checkpoint(&mut state);
    tokio::pin!(checkpoint);
    let written_during_snapshot = tokio::time::timeout(Duration::from_millis(300), &mut checkpoint)
        .await
        .is_ok();
    release.notify_one();
    if !written_during_snapshot {
        checkpoint.await.unwrap();
    }
    snapshot_task.await.unwrap();
    assert!(
        !written_during_snapshot,
        "the checkpoint was written while data snapshot 2 was still being written"
    );

    let newest = h.newest_snapshot().await;
    assert!(newest.checkpoint);
    assert_eq!(
        newest.wal_file_sequence_number,
        data_snapshot.last_wal_sequence_number
    );
    assert_eq!(newest.db_table_and_file_count().2, 2, "both gen1 files");
    let (catalog, restarted) = h.restart().await;
    assert_eq!(
        index_of(&catalog, &restarted),
        index_of(&h.catalog, &h.write_buffer)
    );
}

/// The checkpoint releases the WAL's snapshot permit before its upload: a flush that starts a
/// data snapshot (and so every write) must not wait for that upload.
#[tokio::test]
async fn test_flush_does_not_wait_for_a_checkpoint_upload() {
    // Data snapshot 1, then the checkpoint, 2, which is held.
    let held = SnapshotInfoFilePath::new_checkpoint("test-host", SnapshotSequenceNumber::new(2));
    let reached = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let store = SynchronizedObjectStore::new(Arc::new(InMemory::new())).with_put_notifies(
        held.as_ref().clone(),
        Arc::clone(&release),
        Arc::clone(&reached),
    );
    let h = CompactionHarness::build(Arc::new(store), tag_2497_config()).await;
    h.write_gen1("testtable value=1 10000000000").await;
    let mut state = checkpoint_state(&h);

    let checkpoint = h.service.write_index_checkpoint(&mut state);
    tokio::pin!(checkpoint);
    tokio::select! {
        _ = &mut checkpoint => panic!("test setup: the checkpoint upload was not held"),
        _ = reached.notified() => {}
    }
    h.write_wal_only("testtable value=2 20000000000").await;
    let flushed = tokio::time::timeout(
        Duration::from_secs(1),
        h.write_buffer.wal().force_flush_buffer(),
    )
    .await;
    release.notify_one();
    checkpoint.await.unwrap();

    let (snapshot_done, _, permit) = flushed
        .expect("the flush waited for the checkpoint upload")
        .expect("test setup: no data snapshot");
    snapshot_done.await.expect("snapshot failed");
    drop(permit);
}

/// Shutdown can cancel a checkpoint after it reserved its number: no file is written, the number
/// stays unused, and both a restart and the next checkpoint (which folds past the unused number)
/// still build the same index.
#[tokio::test]
async fn test_a_cancelled_checkpoint_leaves_no_file_and_its_number_unused() {
    let held = SnapshotInfoFilePath::new_checkpoint("test-host", SnapshotSequenceNumber::new(3));
    let reached = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let store = SynchronizedObjectStore::new(Arc::new(InMemory::new())).with_put_notifies(
        held.as_ref().clone(),
        Arc::clone(&release),
        Arc::clone(&reached),
    );
    let h = CompactionHarness::build(Arc::new(store), tag_2497_config()).await;
    h.write_gen1("testtable value=1 10000000000").await;
    h.write_gen1("testtable value=2 20000000000").await;
    let mut state = checkpoint_state(&h);
    {
        let checkpoint = h.service.write_index_checkpoint(&mut state);
        tokio::pin!(checkpoint);
        tokio::select! {
            _ = &mut checkpoint => panic!("test setup: the checkpoint upload was not held"),
            _ = reached.notified() => {}
        }
        // Dropped here, as `start()` drops it when shutdown starts.
    }
    assert!(checkpoints_in_store(&h).await.is_empty());
    let running = index_of(&h.catalog, &h.write_buffer);
    let (catalog, restarted) = h.restart().await;
    assert_eq!(index_of(&catalog, &restarted), running);

    h.write_gen1("testtable value=3 30000000000").await;
    let written = h.service.write_index_checkpoint(&mut state).await.unwrap();
    assert_eq!(
        written,
        SnapshotSequenceNumber::new(5),
        "3 stays unused, 4 is data"
    );
    let running = index_of(&h.catalog, &h.write_buffer);
    let (catalog, restarted) = h.restart().await;
    assert_eq!(index_of(&catalog, &restarted), running);
}

/// A checkpoint that can't be read is skipped: startup falls back to the next older one, and to
/// every snapshot when none can be read. Snapshot info files that can't be read still fail
/// startup.
#[tokio::test]
async fn test_startup_skips_a_checkpoint_it_cannot_read() {
    let h = CompactionHarness::with_config(tag_2497_config()).await;
    h.write_gen1("testtable value=0 10000000000").await;
    let mut state = checkpoint_state(&h);
    let older = h.service.write_index_checkpoint(&mut state).await.unwrap();
    compact_tag_2497_window(&h, 1).await;
    let newer = h.service.write_index_checkpoint(&mut state).await.unwrap();
    compact_tag_2497_window(&h, 2).await;
    let running = index_of(&h.catalog, &h.write_buffer);

    let corrupt = |seq| {
        let store = Arc::clone(&h.object_store);
        async move {
            store
                .put(
                    &SnapshotInfoFilePath::new_checkpoint("test-host", seq),
                    bytes::Bytes::from_static(b"{\"version\":\"1\",\"node_id\":").into(),
                )
                .await
                .unwrap();
        }
    };
    corrupt(newer).await;
    let (catalog, restarted) = h.restart().await;
    assert_eq!(
        index_of(&catalog, &restarted),
        running,
        "from the older checkpoint"
    );

    corrupt(older).await;
    let (catalog, restarted) = h.restart().await;
    assert_eq!(
        index_of(&catalog, &restarted),
        running,
        "from every snapshot"
    );
}

/// The newest checkpoints are kept and older ones deleted; snapshot info files are never
/// deleted.
#[tokio::test]
async fn test_only_the_newest_checkpoints_are_kept() {
    let h = CompactionHarness::with_config(tag_2497_config()).await;
    let mut state = checkpoint_state(&h);
    let mut written = Vec::new();
    for i in 0..5_i64 {
        h.write_gen1(&format!("testtable value={i} {}", (i + 1) * 10_000_000_000))
            .await;
        written.push(h.service.write_index_checkpoint(&mut state).await.unwrap());
    }
    written.reverse();
    assert_eq!(
        checkpoints_in_store(&h).await,
        written[..INDEX_CHECKPOINTS_KEPT].to_vec()
    );
    let info_files = snapshot_paths(&h)
        .await
        .iter()
        .filter(|p| !SnapshotInfoFilePath::is_checkpoint(p))
        .count();
    assert_eq!(info_files, 5, "one data snapshot per write");
}

/// A restart that finds checkpoints keeps counting them: its first checkpoint deletes the
/// oldest beyond the kept number, which an earlier process wrote.
#[tokio::test]
async fn test_checkpoints_written_before_a_restart_are_counted_for_deletion() {
    let h = CompactionHarness::with_config(tag_2497_config()).await;
    let mut state = checkpoint_state(&h);
    for i in 0..INDEX_CHECKPOINTS_KEPT as i64 {
        h.write_gen1(&format!("testtable value={i} {}", (i + 1) * 10_000_000_000))
            .await;
        h.service.write_index_checkpoint(&mut state).await.unwrap();
    }
    let before = checkpoints_in_store(&h).await;

    let (catalog, restarted) = h.restart().await;
    let service = new_test_service(
        tag_2497_config(),
        &catalog,
        &restarted,
        &h.persister,
        &h.executor,
        &h.object_store,
        Arc::clone(&h.time) as _,
    );
    let mut state = IndexCheckpointState::new(restarted.take_index_checkpoint_base().unwrap());
    assert_eq!(state.checkpoints, before);
    let written = service.write_index_checkpoint(&mut state).await.unwrap();
    let mut expected = vec![written];
    expected.extend_from_slice(&before[..INDEX_CHECKPOINTS_KEPT - 1]);
    assert_eq!(checkpoints_in_store(&h).await, expected);
}

/// Due right after startup (unless nothing was ever written, or the index was loaded from a
/// checkpoint that is still the newest snapshot), then once `index_checkpoint_snapshots`
/// numbers have been handed out since the last one.
#[tokio::test]
async fn test_when_a_checkpoint_is_due() {
    let config = CompactionConfig {
        index_checkpoint_snapshots: 3,
        ..tag_2497_config()
    };
    let h = CompactionHarness::with_config(config.clone()).await;
    let mut state = checkpoint_state(&h);
    assert!(
        !h.service.index_checkpoint_due(&state).await,
        "nothing to checkpoint in an empty bucket"
    );
    h.write_gen1("testtable value=1 10000000000").await;
    assert!(
        h.service.index_checkpoint_due(&state).await,
        "first after startup"
    );
    h.service.write_index_checkpoint(&mut state).await.unwrap();
    assert!(!h.service.index_checkpoint_due(&state).await);
    h.write_gen1("testtable value=2 20000000000").await;
    h.write_gen1("testtable value=3 30000000000").await;
    assert!(!h.service.index_checkpoint_due(&state).await, "2 of 3");
    h.write_gen1("testtable value=4 40000000000").await;
    assert!(h.service.index_checkpoint_due(&state).await, "3 of 3");
    h.service.write_index_checkpoint(&mut state).await.unwrap();

    // Restarted from that checkpoint, which is still the newest snapshot: nothing to do.
    let (catalog, restarted) = h.restart().await;
    let service = new_test_service(
        config,
        &catalog,
        &restarted,
        &h.persister,
        &h.executor,
        &h.object_store,
        Arc::clone(&h.time) as _,
    );
    let state = IndexCheckpointState::new(restarted.take_index_checkpoint_base().unwrap());
    assert!(!service.index_checkpoint_due(&state).await);
    // It counts from that checkpoint: one new snapshot doesn't rewrite the whole index.
    write_gen1_with(&catalog, &restarted, "testtable value=5 50000000000").await;
    assert!(!service.index_checkpoint_due(&state).await, "1 of 3");
}

/// The service writes a checkpoint right after it starts, from the index loaded at startup.
#[tokio::test]
async fn test_started_service_writes_a_checkpoint() {
    let h = CompactionHarness::with_config(tag_2497_config()).await;
    h.write_gen1("testtable value=1 10000000000").await;
    let service = Arc::new(
        new_test_service(
            CompactionConfig {
                index_checkpoint_poll_interval: Duration::from_millis(10),
                ..tag_2497_config()
            },
            &h.catalog,
            &h.write_buffer,
            &h.persister,
            &h.executor,
            &h.object_store,
            Arc::clone(&h.time) as _,
        )
        .with_index_checkpoint_base(h.write_buffer.take_index_checkpoint_base().unwrap()),
    );
    let task = Arc::clone(&service).start();
    let mut found = Vec::new();
    for _ in 0..200 {
        found = checkpoints_in_store(&h).await;
        if !found.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    task.abort();
    assert_eq!(found, vec![SnapshotSequenceNumber::new(2)]);
}

/// The index check leaves entries past their table's retention cutoff to retention: they are
/// not checked or pruned, and they don't count towards the prune limit, so expired history
/// can't stop the check from pruning a database's live entries.
#[tokio::test]
async fn test_index_check_leaves_expired_entries_to_retention() {
    let h = CompactionHarness::with_config(CompactionConfig {
        index_check_max_prune_percent: 50,
        ..two_minute_gen2_config()
    })
    .await;
    // Two files that will be past retention, two inside it.
    for (value, seconds) in [(1, 10_i64), (2, 20), (3, 7_200), (4, 7_210)] {
        h.write_gen1(&format!(
            "testtable value={value} {}",
            seconds * 1_000_000_000
        ))
        .await;
    }
    h.catalog
        .set_retention_period_for_database(CompactionHarness::DB, Duration::from_secs(3_600))
        .await
        .unwrap();
    h.set_now(Duration::from_secs(7_300));
    // Every expired object and one live one are missing: 3 of 4 entries stale, but only 1 of
    // the 2 live ones, within the limit.
    let expired = [
        h.indexed_file_at(10_000_000_000),
        h.indexed_file_at(20_000_000_000),
    ];
    let live_stale = h.indexed_file_at(7_200_000_000_000);
    for file in expired.iter().chain([&live_stale]) {
        h.delete_object(file).await;
    }

    h.service.check_index_for_missing_files().await;
    let indexed = h.indexed_files();
    assert!(
        !indexed.contains(&live_stale),
        "the live stale entry is pruned"
    );
    for file in &expired {
        assert!(
            indexed.contains(file),
            "expired entries are left to retention"
        );
    }
    assert_eq!(indexed.len(), 3);
}

/// Like `CompactionHarness::write_gen1`, on a write buffer started by `CompactionHarness::restart`.
async fn write_gen1_with(catalog: &Catalog, write_buffer: &WriteBufferImpl, lp: &str) {
    use data_types::NamespaceName;

    let count = || {
        let db = catalog.db_schema(CompactionHarness::DB).unwrap();
        db.table_definition(CompactionHarness::TABLE)
            .map(|t| {
                write_buffer
                    .persisted_files()
                    .get_files(db.id, t.table_id)
                    .len()
            })
            .unwrap_or(0)
    };
    let before = count();
    write_buffer
        .write_lp(
            NamespaceName::new(CompactionHarness::DB).unwrap(),
            lp,
            iox_time::Time::from_timestamp_nanos(0),
            false,
            crate::Precision::Nanosecond,
            false,
        )
        .await
        .unwrap();
    if let Some((snapshot_done, _, _permit)) = write_buffer.wal().force_flush_buffer().await {
        snapshot_done.await.expect("snapshot failed");
    }
    for _ in 0..100 {
        if count() > before {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no new gen1 file was persisted");
}

/// A checkpoint that can't be read at one start may be readable at the next, and startup then
/// loads only the names that sort before it. So a server that skipped it must number its own
/// snapshots above it: one written under its number or a lower one would be left out later.
#[tokio::test]
async fn test_snapshots_after_skipping_an_unreadable_checkpoint_are_numbered_above_it() {
    let h = CompactionHarness::with_config(tag_2497_config()).await;
    h.write_gen1("testtable value=0 10000000000").await;
    let mut state = checkpoint_state(&h);
    h.service.write_index_checkpoint(&mut state).await.unwrap();
    h.write_gen1("testtable value=1 20000000000").await;
    let newest = h.service.write_index_checkpoint(&mut state).await.unwrap();

    // The newest checkpoint can't be read at this start.
    let path = SnapshotInfoFilePath::new_checkpoint("test-host", newest);
    let saved = h
        .object_store
        .get(&path)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    h.object_store
        .put(&path, bytes::Bytes::from_static(b"{").into())
        .await
        .unwrap();
    let (catalog, restarted) = h.restart().await;
    let numbered_above = restarted.wal().last_snapshot_sequence_number().await >= newest;
    write_gen1_with(&catalog, &restarted, "testtable value=2 30000000000").await;
    let running = index_of(&catalog, &restarted);
    assert_eq!(running[&test_table(&h)].len(), 3);

    // It can be read again at the next start.
    h.object_store.put(&path, saved.into()).await.unwrap();
    let (catalog, again) = h.restart().await;
    assert_eq!(
        index_of(&catalog, &again),
        running,
        "the file written after the checkpoint was skipped must still be loaded"
    );
    assert!(
        numbered_above,
        "new numbers must start above the skipped checkpoint"
    );
}

/// While an index checkpoint waits for the gate, new index snapshots queue behind it. If a
/// snapshot upload keeps failing (so the gate is never free), the checkpoint gives up after
/// `index_checkpoint_gate_timeout` and the snapshots queued behind it go ahead.
#[tokio::test]
async fn test_checkpoint_gives_up_the_gate_so_index_snapshots_keep_going() {
    // Data snapshots 1 to 3, then prune A's snapshot 4, which is held.
    let held = SnapshotInfoFilePath::new("test-host", SnapshotSequenceNumber::new(4));
    let reached = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let store = SynchronizedObjectStore::new(Arc::new(InMemory::new())).with_put_notifies(
        held.as_ref().clone(),
        Arc::clone(&release),
        Arc::clone(&reached),
    );
    let h = CompactionHarness::build(Arc::new(store), tag_2497_config()).await;
    for (value, seconds) in [(1, 10_i64), (2, 20), (3, 30)] {
        h.write_gen1(&format!(
            "testtable value={value} {}",
            seconds * 1_000_000_000
        ))
        .await;
    }
    let (a, b) = (
        h.indexed_file_at(10_000_000_000),
        h.indexed_file_at(20_000_000_000),
    );
    h.delete_object(&a).await;
    h.delete_object(&b).await;
    let service = h.service_with_config(CompactionConfig {
        index_checkpoint_gate_timeout: Duration::from_millis(200),
        ..tag_2497_config()
    });
    let mut state = checkpoint_state(&h);

    let prune_a = service.prune_index_entries(BTreeMap::from([(test_table(&h), vec![a])]));
    tokio::pin!(prune_a);
    tokio::select! {
        _ = &mut prune_a => panic!("test setup: prune A's snapshot was not held"),
        _ = reached.notified() => {}
    }
    let checkpoint = service.write_index_checkpoint(&mut state);
    tokio::pin!(checkpoint);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut checkpoint)
            .await
            .is_err(),
        "test setup: the checkpoint should be waiting for the gate"
    );
    let prune_b = service.prune_index_entries(BTreeMap::from([(test_table(&h), vec![b])]));
    tokio::pin!(prune_b);
    let (checkpoint, pruned_b) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(&mut checkpoint, &mut prune_b)
    })
    .await
    .expect("prune B was held up behind the checkpoint waiting for the gate");
    assert!(checkpoint.is_err(), "the checkpoint should give up");
    assert_eq!(pruned_b.count(), 1);
    release.notify_one();
    assert_eq!(prune_a.await.count(), 1);
}
