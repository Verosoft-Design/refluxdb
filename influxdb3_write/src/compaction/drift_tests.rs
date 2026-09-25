//! Tests for the per-job catalog drift check in [`CompactionService::execute_compaction_job`].

use super::*;
use crate::Bufferer;
use crate::Precision;
use crate::persister::{DEFAULT_OBJECT_STORE_URL, Persister};
use crate::write_buffer::{WriteBufferImpl, WriteBufferImplArgs};
use data_types::NamespaceName;
use influxdb3_cache::distinct_cache::DistinctCacheProvider;
use influxdb3_cache::last_cache::LastCacheProvider;
use influxdb3_shutdown::ShutdownManager;
use influxdb3_wal::WalConfig;
use iox_time::MockProvider;
use metric::Registry;
use object_store::memory::InMemory;

/// Entries whose object is missing or whose size does not match the object are pruned from the
/// index (by id, so a valid entry sharing the stale entry's path survives) and excluded from the
/// job.
#[tokio::test]
async fn drift_check_prunes_missing_and_size_mismatched_entries() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let time_provider: Arc<dyn TimeProvider> =
        Arc::new(MockProvider::new(iox_time::Time::from_timestamp_nanos(0)));
    let catalog = Arc::new(
        Catalog::new(
            "test-host",
            Arc::clone(&object_store),
            Arc::clone(&time_provider),
            Default::default(),
        )
        .await
        .unwrap(),
    );
    let db_name = "testdb";
    let table_name = "testtable";
    catalog.create_database(db_name).await.unwrap();
    let db_id = catalog.db_name_to_id(db_name).unwrap();

    let persister = Arc::new(Persister::new(
        Arc::clone(&object_store),
        "test-host",
        Arc::clone(&time_provider),
    ));
    let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog))
        .await
        .unwrap();
    let distinct_cache =
        DistinctCacheProvider::new_from_catalog(Arc::clone(&time_provider), Arc::clone(&catalog))
            .await
            .unwrap();
    let write_buffer = WriteBufferImpl::new(WriteBufferImplArgs {
        persister: Arc::clone(&persister),
        catalog: Arc::clone(&catalog),
        last_cache,
        distinct_cache,
        time_provider: Arc::clone(&time_provider),
        executor: Arc::new(Executor::new_testing()),
        wal_config: WalConfig::test_config(),
        parquet_cache: None,
        metric_registry: Arc::new(Registry::default()),
        snapshotted_wal_files_to_keep: 10,
        query_file_limit: None,
        n_snapshots_to_load_on_start: 1,
        shutdown: ShutdownManager::new_testing().register(),
        wal_replay_concurrency_limit: None,
    })
    .await
    .unwrap();

    write_buffer
        .write_lp(
            NamespaceName::new(db_name).unwrap(),
            "testtable value=1 10000000000\ntesttable value=2 30000000000",
            iox_time::Time::from_timestamp_nanos(0),
            false,
            Precision::Nanosecond,
            false,
        )
        .await
        .unwrap();
    let _ = write_buffer.wal().force_flush_buffer().await;

    let table_def = catalog
        .db_schema(db_name)
        .unwrap()
        .table_definition(table_name)
        .unwrap();
    let table_id = table_def.table_id;
    let persisted_files = write_buffer.persisted_files();

    // Persisting happens in the background after the flush; wait for it.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let valid_files = loop {
        let files = persisted_files.get_files(db_id, table_id);
        if !files.is_empty() {
            break files;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "should have persisted files"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let valid = valid_files[0].clone();

    // A stale entry for the same path as a valid one: the damaged state left by overwriting
    // compactions. It is not in the index (the index no longer admits same-path duplicates), but
    // a job built before the fix could still carry it.
    let stale_same_path = ParquetFile {
        id: ParquetFileId::new(),
        size_bytes: valid.size_bytes + 20,
        ..valid.clone()
    };

    // An indexed entry whose object was overwritten with different content.
    let overwritten_path =
        "test-host/dbs/testdb-0/testtable-0/2026-09-18/00-00/overwritten.parquet";
    object_store
        .put(&ObjPath::from(overwritten_path), vec![0_u8; 100].into())
        .await
        .unwrap();
    let overwritten = ParquetFile {
        id: ParquetFileId::new(),
        path: overwritten_path.to_string(),
        size_bytes: 90,
        ..valid.clone()
    };
    persisted_files.add_persisted_file(&db_id, &table_id, &overwritten);

    // An indexed entry whose object does not exist.
    let missing = ParquetFile {
        id: ParquetFileId::new(),
        path: "test-host/dbs/testdb-0/testtable-0/2026-09-18/00-00/missing.parquet".to_string(),
        ..valid.clone()
    };
    persisted_files.add_persisted_file(&db_id, &table_id, &missing);

    let mut expected_remaining = valid_files.clone();
    expected_remaining.sort_by_key(|f| f.id);

    let sort_key = if table_def.sort_key.is_empty() {
        SortKey::from_columns(vec!["time"])
    } else {
        table_def.sort_key.clone()
    };
    let job = CompactionJob {
        database_id: db_id,
        table_id,
        table_name: Arc::clone(&table_def.table_name),
        source_generation: 1,
        target_generation: 2,
        files: vec![
            valid.clone(),
            stale_same_path,
            overwritten.clone(),
            missing.clone(),
        ],
        schema: table_def.schema.clone(),
        sort_key,
    };

    // A minimum the job cannot reach after the drift check, so the job stops right after it and
    // the test does not depend on the compaction write path.
    let service = CompactionService::new(
        CompactionConfig {
            enabled: true,
            interval: Duration::from_secs(1),
            max_files_per_run: 10,
            min_files_for_compaction: 100,
            generation_durations: HashMap::from([(2, Duration::from_secs(120))]),
        },
        Arc::clone(&catalog),
        Arc::clone(&write_buffer) as Arc<dyn WriteBuffer>,
        Arc::clone(&persisted_files),
        Arc::new(Executor::new_testing()),
        HashMap::new(),
        Arc::clone(&object_store),
        ObjectStoreUrl::parse(DEFAULT_OBJECT_STORE_URL).unwrap(),
        "test-host",
        Arc::clone(&time_provider),
        ShutdownManager::new_testing().register(),
    );

    let result = service.execute_compaction_job(job).await.unwrap();
    assert!(result.compacted_files.is_empty());
    assert!(result.deleted_files.is_empty());

    let mut remaining = persisted_files.get_files(db_id, table_id);
    remaining.sort_by_key(|f| f.id);
    assert_eq!(
        remaining, expected_remaining,
        "the overwritten and missing entries are pruned; the valid entry sharing a path with a \
         stale entry is kept"
    );

    // Stale entries are only dropped from the index; the objects are left alone.
    object_store
        .head(&ObjPath::from(valid.path.as_str()))
        .await
        .unwrap();
    object_store
        .head(&ObjPath::from(overwritten_path))
        .await
        .unwrap();
}
