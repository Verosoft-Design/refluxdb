use chrono::prelude::*;
use influxdb3_catalog::catalog::CatalogSequenceNumber;
use influxdb3_wal::{SnapshotSequenceNumber, WalFileSequenceNumber};
use object_store::path::Path as ObjPath;
use std::ops::Deref;

/// File extension for catalog files
pub const CATALOG_LOG_FILE_EXTENSION: &str = "catalog";

/// File extension for catalog files
pub const CATALOG_SNAPSHOT_FILE_EXTENSION: &str = "catalog.snapshot";

/// File extension for parquet files
pub const PARQUET_FILE_EXTENSION: &str = "parquet";

/// File extension for snapshot info files
pub const SNAPSHOT_INFO_FILE_EXTENSION: &str = "info.json";

/// File extension for index checkpoints, which are kept with the snapshot info files (see
/// [`SnapshotInfoFilePath::new_checkpoint`]).
pub const INDEX_CHECKPOINT_FILE_EXTENSION: &str = "checkpoint.json";

fn object_store_file_stem(n: u64) -> u64 {
    u64::MAX - n
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogFilePath(ObjPath);

impl CatalogFilePath {
    pub fn log(host_prefix: &str, catalog_sequence_number: CatalogSequenceNumber) -> Self {
        let num = u64::MAX - catalog_sequence_number.get();
        let path = ObjPath::from(format!(
            "{host_prefix}/catalogs/{num:020}.{CATALOG_LOG_FILE_EXTENSION}",
        ));
        Self(path)
    }

    pub fn snapshot(host_prefix: &str, catalog_sequence_number: CatalogSequenceNumber) -> Self {
        let num = u64::MAX - catalog_sequence_number.get();
        let path = ObjPath::from(format!(
            "{host_prefix}/catalogs/{num:020}.{CATALOG_SNAPSHOT_FILE_EXTENSION}",
        ));
        Self(path)
    }

    pub fn dir(host_prefix: &str) -> Self {
        Self(ObjPath::from(format!("{host_prefix}/catalogs")))
    }
}

impl Deref for CatalogFilePath {
    type Target = ObjPath;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<ObjPath> for CatalogFilePath {
    fn as_ref(&self) -> &ObjPath {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParquetFilePath(ObjPath);

impl ParquetFilePath {
    /// Generate a parquet file path using the given arguments. This will convert the provided
    /// `chunk_time` into a date time string with format `'YYYY-MM-DD/HH-MM'`
    pub fn new(
        host_prefix: &str,
        db_name: &str,
        db_id: u32,
        table_name: &str,
        table_id: u32,
        chunk_time: i64,
        wal_file_sequence_number: WalFileSequenceNumber,
    ) -> Self {
        let date_time = DateTime::<Utc>::from_timestamp_nanos(chunk_time);
        let path = ObjPath::from(format!(
            "{host_prefix}/dbs/{db_name}-{db_id}/{table_name}-{table_id}/{date_string}/{wal_seq:010}.{ext}",
            date_string = date_time.format("%Y-%m-%d/%H-%M"),
            wal_seq = wal_file_sequence_number.as_u64(),
            ext = PARQUET_FILE_EXTENSION
        ));
        Self(path)
    }
}

impl Deref for ParquetFilePath {
    type Target = ObjPath;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<ObjPath> for ParquetFilePath {
    fn as_ref(&self) -> &ObjPath {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotInfoFilePath(ObjPath);

impl SnapshotInfoFilePath {
    pub fn new(host_prefix: &str, snapshot_sequence_number: SnapshotSequenceNumber) -> Self {
        let path = ObjPath::from(format!(
            "{host_prefix}/snapshots/{:020}.{}",
            object_store_file_stem(snapshot_sequence_number.as_u64()),
            SNAPSHOT_INFO_FILE_EXTENSION
        ));
        Self(path)
    }

    /// Where the index checkpoint with this sequence number is stored: in the snapshots folder,
    /// named like a snapshot info file but with its own extension. Images that predate
    /// checkpoints list and load it as an ordinary snapshot; startup finds the newest one by
    /// name alone.
    pub fn new_checkpoint(
        host_prefix: &str,
        snapshot_sequence_number: SnapshotSequenceNumber,
    ) -> Self {
        let path = ObjPath::from(format!(
            "{host_prefix}/snapshots/{:020}.{}",
            object_store_file_stem(snapshot_sequence_number.as_u64()),
            INDEX_CHECKPOINT_FILE_EXTENSION
        ));
        Self(path)
    }

    /// Whether `path` names an index checkpoint.
    pub fn is_checkpoint(path: &ObjPath) -> bool {
        path.filename()
            .is_some_and(|name| name.ends_with(&format!(".{INDEX_CHECKPOINT_FILE_EXTENSION}")))
    }

    /// The sequence number in the name of a snapshot info file or index checkpoint.
    pub fn sequence_number(path: &ObjPath) -> Option<SnapshotSequenceNumber> {
        let stem: u64 = path.filename()?.split('.').next()?.parse().ok()?;
        Some(SnapshotSequenceNumber::new(object_store_file_stem(stem)))
    }

    pub fn dir(host_prefix: &str) -> Self {
        Self(ObjPath::from(format!("{host_prefix}/snapshots")))
    }
}

impl Deref for SnapshotInfoFilePath {
    type Target = ObjPath;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<ObjPath> for SnapshotInfoFilePath {
    fn as_ref(&self) -> &ObjPath {
        &self.0
    }
}

#[test]
fn catalog_file_path_new() {
    assert_eq!(
        *CatalogFilePath::log("my_host", CatalogSequenceNumber::new(0)),
        ObjPath::from("my_host/catalogs/18446744073709551615.catalog")
    );
}

#[test]
fn parquet_file_path_new() {
    assert_eq!(
        *ParquetFilePath::new(
            "my_host",
            "my_db",
            0,
            "my_table",
            0,
            Utc.with_ymd_and_hms(2038, 1, 19, 3, 14, 7)
                .unwrap()
                .timestamp_nanos_opt()
                .unwrap(),
            WalFileSequenceNumber::new(1337),
        ),
        ObjPath::from("my_host/dbs/my_db-0/my_table-0/2038-01-19/03-14/0000001337.parquet")
    );
}

#[test]
fn parquet_file_percent_encoded() {
    assert_eq!(
        ParquetFilePath::new(
            "..",
            "..",
            0,
            "..",
            0,
            Utc.with_ymd_and_hms(2038, 1, 19, 3, 14, 7)
                .unwrap()
                .timestamp_nanos_opt()
                .unwrap(),
            WalFileSequenceNumber::new(100),
        )
        .as_ref()
        .as_ref(),
        "%2E%2E/dbs/..-0/..-0/2038-01-19/03-14/0000000100.parquet"
    );
}

#[test]
fn snapshot_info_file_path_new() {
    assert_eq!(
        *SnapshotInfoFilePath::new("my_host", SnapshotSequenceNumber::new(0)),
        ObjPath::from("my_host/snapshots/18446744073709551615.info.json")
    );
}

#[test]
fn index_checkpoint_path_sorts_with_the_snapshot_of_the_same_number() {
    let seq = SnapshotSequenceNumber::new(11_000_000_000_000_000_123);
    let checkpoint = SnapshotInfoFilePath::new_checkpoint("my_host", seq);
    assert_eq!(
        *checkpoint,
        ObjPath::from("my_host/snapshots/07446744073709551492.checkpoint.json")
    );
    assert!(SnapshotInfoFilePath::is_checkpoint(&checkpoint));
    assert!(!SnapshotInfoFilePath::is_checkpoint(
        &SnapshotInfoFilePath::new("my_host", seq)
    ));
    assert_eq!(
        SnapshotInfoFilePath::sequence_number(&checkpoint),
        Some(seq)
    );
    assert_eq!(
        SnapshotInfoFilePath::sequence_number(&SnapshotInfoFilePath::new("my_host", seq)),
        Some(seq)
    );
    // Startup sorts names ascending, newest first: the snapshot after the checkpoint sorts
    // before it, the one before it after.
    let newer = SnapshotInfoFilePath::new("my_host", seq.next());
    let older = SnapshotInfoFilePath::new("my_host", SnapshotSequenceNumber::new(seq.as_u64() - 1));
    assert!(newer.as_ref() < checkpoint.as_ref());
    assert!(checkpoint.as_ref() < older.as_ref());
}
