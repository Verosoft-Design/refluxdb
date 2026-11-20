use super::*;
use crate::persister::Persister;
use crate::{ParquetFile, ParquetFileId, PersistedSnapshotVersion};
use influxdb3_catalog::catalog::CatalogSequenceNumber;
use influxdb3_wal::{SnapshotSequenceNumber, WalFileSequenceNumber};
use iox_time::{MockProvider, Time};
use object_store::memory::InMemory;

const NODE: &str = "hosta";

fn file(path: &str) -> ParquetFile {
    ParquetFile {
        id: ParquetFileId::new(),
        path: path.to_string(),
        size_bytes: 1,
        row_count: 1,
        chunk_time: 0,
        min_time: 0,
        max_time: 0,
    }
}

async fn persist(
    persister: &Persister,
    seq: u64,
    added: &[&ParquetFile],
    removed: &[&ParquetFile],
    deletes_removed_objects: bool,
) {
    let (db_id, table_id) = (DbId::from(0), TableId::from(0));
    let mut snapshot = PersistedSnapshot::new(
        NODE.to_string(),
        SnapshotSequenceNumber::new(seq),
        WalFileSequenceNumber::new(seq),
        CatalogSequenceNumber::new(0),
    );
    for file in added {
        snapshot.add_parquet_file(db_id, table_id, (*file).clone());
    }
    snapshot
        .removed_files
        .entry(db_id)
        .or_default()
        .tables
        .entry(table_id)
        .or_default()
        .extend(removed.iter().map(|f| (*f).clone()));
    snapshot.deletes_removed_objects = deletes_removed_objects;
    persister
        .persist_snapshot(&PersistedSnapshotVersion::V1(snapshot))
        .await
        .unwrap();
}

/// Startup deletes the objects of the marked removals (retention and hard deletes) that the
/// loaded index no longer holds. It leaves a removed entry's path that a newer entry still
/// holds, and the removals of unmarked snapshots, such as the index check's prunes, whose
/// objects are kept on purpose.
#[tokio::test]
async fn startup_deletes_only_unindexed_paths_of_marked_removals() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let persister = Persister::new(
        Arc::clone(&object_store),
        NODE,
        Arc::new(MockProvider::new(Time::from_timestamp_nanos(0))) as _,
    );
    let expired = file("hosta/dbs/db-0/t-0/1970-01-01/00-00/0000000001.parquet");
    let replaced = file("hosta/dbs/db-0/t-0/1970-01-01/00-00/0000000002.parquet");
    let pruned = file("hosta/dbs/db-0/t-0/1970-01-01/00-00/0000000003.parquet");
    // A newer write to `replaced`'s path: the index keeps this entry, and the object is its.
    let replacing = ParquetFile {
        id: ParquetFileId::new(),
        ..replaced.clone()
    };

    persist(&persister, 1, &[&expired, &replaced, &pruned], &[], false).await;
    persist(&persister, 2, &[&replacing], &[], false).await;
    // Retention removes `expired`, and the old entry for `replaced`'s path.
    persist(&persister, 3, &[], &[&expired, &replaced], true).await;
    // The index check prunes `pruned`.
    persist(&persister, 4, &[], &[&pruned], false).await;

    let loaded = persister.load_index(usize::MAX).await.unwrap();
    assert_eq!(
        loaded.removals.unindexed(&loaded.fold),
        vec![Path::from(expired.path.as_str())]
    );
    let mut indexed: Vec<_> = loaded.fold.files().into_values().flatten().collect();
    indexed.sort_by_key(|f| f.id);
    assert_eq!(indexed, vec![replacing]);
}
