use super::*;
use crate::Precision;
use crate::paths::SnapshotInfoFilePath;
use crate::test_helpers::TestWriter;
use crate::write_buffer::validator::WriteValidator;
use datafusion_util::config::register_iox_object_store;
use executor::{DedicatedExecutor, register_current_runtime_for_io};
use influxdb3_catalog::resource::CatalogResource;
use influxdb3_test_helpers::object_store::SynchronizedObjectStore;
use influxdb3_wal::{Gen1Duration, SnapshotSequenceNumber, WalFileSequenceNumber};
use iox_query::exec::{ExecutorConfig, PerQueryMemoryPoolConfig};
use iox_time::{MockProvider, Time, TimeProvider};
use object_store::ObjectStore;
use object_store::memory::InMemory;
use parquet::arrow::arrow_reader;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet_file::storage::{ParquetStorage, StorageId};
use std::num::NonZeroUsize;
use tokio::sync::Notify;

const NODE: &str = "hosta";

/// A [`QueryableBuffer`] over `object_store`, and the catalog it writes through. The catalog, the
/// persister and the distinct cache take the time from `time_provider`.
async fn queryable_buffer_over(
    object_store: Arc<dyn ObjectStore>,
    time_provider: Arc<MockProvider>,
) -> (QueryableBuffer, Arc<Catalog>) {
    let metrics = Arc::new(metric::Registry::default());
    let parquet_store =
        ParquetStorage::new(Arc::clone(&object_store), StorageId::from("influxdb3"));
    let exec = Arc::new(Executor::new_with_config_and_executor(
        ExecutorConfig {
            target_query_partitions: NonZeroUsize::new(1).unwrap(),
            object_stores: [&parquet_store]
                .into_iter()
                .map(|store| (store.id(), Arc::clone(store.object_store())))
                .collect(),
            metric_registry: Arc::clone(&metrics),
            // Default to 1gb
            mem_pool_size: 1024 * 1024 * 1024, // 1024 (b/kb) * 1024 (kb/mb) * 1024 (mb/gb)
            per_query_mem_pool_config: PerQueryMemoryPoolConfig::Disabled,
            heap_memory_limit: None,
        },
        DedicatedExecutor::new_testing(),
    ));
    let runtime_env = exec.new_context().inner().runtime_env();
    register_iox_object_store(runtime_env, parquet_store.id(), Arc::clone(&object_store));
    register_current_runtime_for_io();

    let catalog = Arc::new(
        Catalog::new(
            NODE,
            Arc::clone(&object_store),
            Arc::clone(&time_provider) as _,
            Default::default(),
        )
        .await
        .unwrap(),
    );
    let persister = Arc::new(Persister::new(
        Arc::clone(&object_store),
        NODE,
        Arc::clone(&time_provider) as _,
    ));
    let queryable_buffer = QueryableBuffer::new(QueryableBufferArgs {
        executor: exec,
        catalog: Arc::clone(&catalog),
        persister,
        last_cache_provider: LastCacheProvider::new_from_catalog(Arc::clone(&catalog))
            .await
            .unwrap(),
        distinct_cache_provider: DistinctCacheProvider::new_from_catalog(
            time_provider as _,
            Arc::clone(&catalog),
        )
        .await
        .unwrap(),
        persisted_files: Arc::new(PersistedFiles::new()),
        parquet_cache: None,
    });
    (queryable_buffer, catalog)
}

#[tokio::test]
async fn snapshot_works_with_not_all_columns_in_buffer() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let (queryable_buffer, catalog) =
        queryable_buffer_over(Arc::clone(&object_store), Arc::clone(&time_provider)).await;

    let db = data_types::NamespaceName::new("testdb").unwrap();

    // create the initial write with two tags
    let val = WriteValidator::initialize(db.clone(), Arc::clone(&catalog)).unwrap();
    let lp = format!(
        "foo,t2=a,t1=b f1=1i {}",
        time_provider.now().timestamp_nanos()
    );

    let lines = val
        .v1_parse_lines_and_catalog_updates(&lp, false, time_provider.now(), Precision::Nanosecond)
        .unwrap()
        .commit_catalog_changes()
        .await
        .unwrap()
        .unwrap_success()
        .convert_lines_to_buffer(Gen1Duration::new_1m());
    let batch: WriteBatch = lines.into();
    let wal_contents = WalContents {
        persist_timestamp_ms: 0,
        min_timestamp_ns: batch.min_time_ns,
        max_timestamp_ns: batch.max_time_ns,
        wal_file_number: WalFileSequenceNumber::new(1),
        ops: vec![WalOp::Write(batch)],
        snapshot: None,
    };
    let end_time =
        wal_contents.max_timestamp_ns + Gen1Duration::new_1m().as_duration().as_nanos() as i64;

    // write the lp into the buffer
    queryable_buffer.notify(Arc::new(wal_contents)).await;

    // now force a snapshot, persisting the data to parquet file. Also, buffer up a new write
    let snapshot_sequence_number = SnapshotSequenceNumber::new(1);
    let snapshot_details = SnapshotDetails {
        snapshot_sequence_number,
        end_time_marker: end_time,
        first_wal_sequence_number: WalFileSequenceNumber::new(1),
        last_wal_sequence_number: WalFileSequenceNumber::new(2),
        forced: false,
    };

    // create another write, this time with only one tag, in a different gen1 block
    let lp = "foo,t2=b f1=1i 240000000000";
    let val = WriteValidator::initialize(db, Arc::clone(&catalog)).unwrap();

    let lines = val
        .v1_parse_lines_and_catalog_updates(lp, false, time_provider.now(), Precision::Nanosecond)
        .unwrap()
        .commit_catalog_changes()
        .await
        .unwrap()
        .unwrap_success()
        .convert_lines_to_buffer(Gen1Duration::new_1m());
    let batch: WriteBatch = lines.into();
    let wal_contents = WalContents {
        persist_timestamp_ms: 0,
        min_timestamp_ns: batch.min_time_ns,
        max_timestamp_ns: batch.max_time_ns,
        wal_file_number: WalFileSequenceNumber::new(2),
        ops: vec![WalOp::Write(batch)],
        snapshot: None,
    };
    let end_time =
        wal_contents.max_timestamp_ns + Gen1Duration::new_1m().as_duration().as_nanos() as i64;

    let details = queryable_buffer
        .notify_and_snapshot(Arc::new(wal_contents), snapshot_details)
        .await;
    let _details = details.await.unwrap();

    // validate we have a single persisted file
    let db = catalog.db_schema("testdb").unwrap();
    let table = db.table_definition("foo").unwrap();
    assert_eq!(
        table.sort_key,
        SortKey::from_columns(vec!["t2", "t1", "time"])
    );
    let files = queryable_buffer
        .persisted_files
        .get_files(db.id, table.table_id);
    assert_eq!(files.len(), 1);

    // now force another snapshot, persisting the data to parquet file
    let snapshot_sequence_number = SnapshotSequenceNumber::new(2);
    let snapshot_details = SnapshotDetails {
        snapshot_sequence_number,
        end_time_marker: end_time,
        first_wal_sequence_number: WalFileSequenceNumber::new(3),
        last_wal_sequence_number: WalFileSequenceNumber::new(3),
        forced: false,
    };
    queryable_buffer
        .notify_and_snapshot(
            Arc::new(WalContents {
                persist_timestamp_ms: 0,
                min_timestamp_ns: 0,
                max_timestamp_ns: 0,
                wal_file_number: WalFileSequenceNumber::new(3),
                ops: vec![],
                snapshot: Some(snapshot_details),
            }),
            snapshot_details,
        )
        .await
        .await
        .unwrap();

    // validate we have two persisted files
    let files = queryable_buffer
        .persisted_files
        .get_files(db.id, table.table_id);
    assert_eq!(files.len(), 2);

    // Verify the `iox::series::key` metadata is present in the parquet file
    {
        let path = Path::from(files[0].path.as_str());
        let res = object_store
            .get(&path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let metadata =
            arrow_reader::ArrowReaderMetadata::load(&res, ArrowReaderOptions::new()).unwrap();
        let schema: Schema = Schema::try_from(Arc::clone(metadata.schema())).unwrap();
        let primary_key = schema.primary_key();
        assert_eq!(primary_key, &["t2", "t1", "time"]);
    }
}

/// This test validates that buffer replay ignores data from deleted tables.
#[tokio::test]
async fn snapshot_skips_deleted_table() {
    // Setup test infrastructure
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let (queryable_buffer, catalog) = queryable_buffer_over(object_store, time_provider).await;

    let writer = TestWriter::new_with_catalog(Arc::clone(&catalog));
    let lines1 = writer
        .write_lp_to_write_batch("table1,tag=a value=1i 1000", 0)
        .await;
    let lines2 = writer
        .write_lp_to_write_batch("table2,tag=b value=2i 2000", 0)
        .await;

    let db_schema = catalog.db_schema(TestWriter::DB_NAME).unwrap();
    let db_id = db_schema.id();
    let table1_id = db_schema.table_name_to_id("table1").unwrap();
    let table2_id = db_schema.table_name_to_id("table2").unwrap();

    // Soft delete the second table
    use influxdb3_catalog::catalog::HardDeletionTime;
    catalog
        .soft_delete_table(TestWriter::DB_NAME, "table2", HardDeletionTime::Now)
        .await
        .unwrap();

    let snapshot_details = SnapshotDetails {
        snapshot_sequence_number: SnapshotSequenceNumber::new(1),
        end_time_marker: 1000,
        first_wal_sequence_number: WalFileSequenceNumber::new(1),
        last_wal_sequence_number: WalFileSequenceNumber::new(2),
        forced: false,
    };

    let wal_contents = influxdb3_wal::create::wal_contents_with_snapshot(
        (0, 100, 1),
        [
            influxdb3_wal::create::write_batch_op(lines1),
            influxdb3_wal::create::write_batch_op(lines2),
        ],
        snapshot_details,
    );

    queryable_buffer
        .notify_and_snapshot(Arc::new(wal_contents), snapshot_details)
        .await
        .await
        .unwrap();

    // Verify only table1 has persisted files
    let files1 = queryable_buffer.persisted_files.get_files(db_id, table1_id);
    assert_eq!(files1.len(), 1, "Should have 1 persisted file for table1");

    let files2 = queryable_buffer.persisted_files.get_files(db_id, table2_id);
    assert_eq!(
        files2.len(),
        0,
        "Soft deleted table should not have persisted files"
    );
}

/// Every index entry a server starting on `object_store` would load, through the same
/// snapshot load startup uses.
async fn index_after_restart(object_store: &Arc<dyn ObjectStore>) -> Vec<ParquetFile> {
    let persister = Persister::new(
        Arc::clone(object_store),
        NODE,
        Arc::new(MockProvider::new(Time::from_timestamp_nanos(0))) as _,
    );
    let loaded = persister.load_index(usize::MAX).await.unwrap();
    loaded.fold.files().into_values().flatten().collect()
}

/// Retention removes a file from the index during a flush, and the snapshot that records the
/// removal is written later in that flush. If the process stops before that snapshot is
/// written, the next start loads the older snapshots, which still list the file, so its object
/// must still exist (TAG-2753). The snapshot's upload is held at the store, and the restart
/// reads the store as the stopped process left it. Once the snapshot is written, the object is
/// deleted.
#[tokio::test]
async fn retention_deletes_objects_only_after_the_snapshot_that_records_their_removal() {
    // Snapshot 2's upload waits at the store until `release_snapshot_2` is notified.
    let snapshot_2_reached = Arc::new(Notify::new());
    let release_snapshot_2 = Arc::new(Notify::new());
    let object_store: Arc<dyn ObjectStore> = Arc::new(
        SynchronizedObjectStore::new(Arc::new(InMemory::new())).with_put_notifies(
            Path::clone(&SnapshotInfoFilePath::new(
                NODE,
                SnapshotSequenceNumber::new(2),
            )),
            Arc::clone(&release_snapshot_2),
            Arc::clone(&snapshot_2_reached),
        ),
    );
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let (queryable_buffer, catalog) =
        queryable_buffer_over(Arc::clone(&object_store), Arc::clone(&time_provider)).await;

    // Snapshot 1 persists a file and the snapshot that lists it.
    let writer = TestWriter::new_with_catalog(Arc::clone(&catalog));
    let lines = writer
        .write_lp_to_write_batch("cpu,tag=a value=1i 1000", 0)
        .await;
    let snapshot_1 = SnapshotDetails {
        snapshot_sequence_number: SnapshotSequenceNumber::new(1),
        end_time_marker: 1000,
        first_wal_sequence_number: WalFileSequenceNumber::new(1),
        last_wal_sequence_number: WalFileSequenceNumber::new(1),
        forced: false,
    };
    queryable_buffer
        .notify_and_snapshot(
            Arc::new(influxdb3_wal::create::wal_contents_with_snapshot(
                (0, 1000, 1),
                [influxdb3_wal::create::write_batch_op(lines)],
                snapshot_1,
            )),
            snapshot_1,
        )
        .await
        .await
        .unwrap();
    let db_schema = catalog.db_schema(TestWriter::DB_NAME).unwrap();
    let db_id = db_schema.id();
    let table_id = db_schema.table_name_to_id("cpu").unwrap();
    let files = queryable_buffer.persisted_files.get_files(db_id, table_id);
    assert_eq!(files.len(), 1, "snapshot 1 persists one file");
    let file = files[0].clone();
    let file_path = Path::from(file.path.as_str());
    object_store.head(&file_path).await.unwrap();

    // The file's rows fall out of the database's retention period.
    catalog
        .set_retention_period_for_database(TestWriter::DB_NAME, Duration::from_secs(60 * 60))
        .await
        .unwrap();
    time_provider.set(Time::from_timestamp_nanos(
        Duration::from_secs(2 * 60 * 60).as_nanos() as i64,
    ));

    // Snapshot 2 removes the file from the index and records the removal. Its upload is held,
    // so the process stops before the snapshot is written.
    let snapshot_2 = SnapshotDetails {
        snapshot_sequence_number: SnapshotSequenceNumber::new(2),
        end_time_marker: 1000,
        first_wal_sequence_number: WalFileSequenceNumber::new(2),
        last_wal_sequence_number: WalFileSequenceNumber::new(2),
        forced: false,
    };
    let snapshot_2_done = queryable_buffer
        .notify_and_snapshot(
            Arc::new(influxdb3_wal::create::wal_contents_with_snapshot(
                (0, 0, 2),
                [],
                snapshot_2,
            )),
            snapshot_2,
        )
        .await;
    assert!(
        queryable_buffer
            .persisted_files
            .get_files(db_id, table_id)
            .is_empty(),
        "retention removed the file from the index"
    );
    // Once the upload is reached, any delete spawned before the snapshot task (the order before
    // TAG-2753) has run: this current-thread runtime polls tasks in the order they were spawned.
    tokio::time::timeout(Duration::from_secs(10), snapshot_2_reached.notified())
        .await
        .expect("snapshot 2's upload reaches the store");

    // A restart loads snapshot 1, which lists the file: its object must still be there.
    let indexed = index_after_restart(&object_store).await;
    assert_eq!(indexed, vec![file.clone()]);
    for entry in &indexed {
        if let Err(e) = object_store.head(&Path::from(entry.path.as_str())).await {
            panic!(
                "after a restart, the index entry {} points at a deleted object: {e}",
                entry.path
            );
        }
    }

    // Without the stop, snapshot 2 is written and then the object is deleted.
    release_snapshot_2.notify_one();
    snapshot_2_done.await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while object_store.head(&file_path).await.is_ok() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the file's object is deleted once snapshot 2 is written");
    assert!(index_after_restart(&object_store).await.is_empty());
}
