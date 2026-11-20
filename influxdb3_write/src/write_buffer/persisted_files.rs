//! This tracks what files have been persisted by the write buffer, limited to the last 72 hours.
//! When queries come in they will combine whatever chunks exist from `QueryableBuffer` with
//! the persisted files to get the full set of data to query.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::deleter::ObjectDeleter;
use crate::{ChunkFilter, DatabaseTables};
use crate::{ParquetFile, ParquetFileId, PersistedSnapshot};
use hashbrown::{HashMap, HashSet};
use influxdb3_catalog::catalog::{Catalog, CatalogSequenceNumber};
use influxdb3_id::TableId;
use influxdb3_id::{DbId, SerdeVecMap};
use influxdb3_telemetry::ParquetMetrics;
use influxdb3_wal::{SnapshotSequenceNumber, WalFileSequenceNumber};
use observability_deps::tracing::warn;
use parking_lot::{RwLock, RwLockUpgradableReadGuard};

type DatabaseToTables = HashMap<DbId, TableToFiles>;
type TableToFiles = HashMap<TableId, Vec<ParquetFile>>;

#[derive(Debug, Default)]
pub struct PersistedFiles {
    inner: RwLock<Inner>,
}

#[derive(Debug)]
enum DeletedTables {
    /// All tables in the database are marked for deletion
    All,
    /// A list of tables in the database that are marked for deletion
    List(HashSet<TableId>),
}

impl ObjectDeleter for PersistedFiles {
    fn delete_database(&self, db_id: DbId) {
        let mut inner = self.inner.write();
        inner.deleted_data.insert(db_id, DeletedTables::All);
    }

    fn delete_table(&self, db_id: DbId, table_id: TableId) {
        let mut inner = self.inner.write();
        match inner.deleted_data.entry(db_id) {
            hashbrown::hash_map::Entry::Occupied(mut entry) => {
                match entry.get_mut() {
                    DeletedTables::All => (), // already marked for deletion
                    DeletedTables::List(tables) => {
                        tables.insert(table_id);
                    }
                }
            }
            hashbrown::hash_map::Entry::Vacant(entry) => {
                entry.insert(DeletedTables::List(HashSet::from([table_id])));
            }
        }
    }
}

impl PersistedFiles {
    pub fn new() -> Self {
        Default::default()
    }
    /// Create a new `PersistedFiles` from a list of persisted snapshots
    pub fn new_from_persisted_snapshots(persisted_snapshots: Vec<PersistedSnapshot>) -> Self {
        let inner = Inner::new_from_persisted_snapshots(persisted_snapshots);
        Self {
            inner: RwLock::new(inner),
        }
    }

    /// Create a new `PersistedFiles` from snapshots folded oldest to newest (see
    /// `Persister::load_index`).
    pub(crate) fn from_fold(fold: SnapshotFold) -> Self {
        Self {
            inner: RwLock::new(Inner::from_fold(fold)),
        }
    }

    /// The WAL number of the newest completed data snapshot: every WAL file up to it is in
    /// parquet (or held nothing to persist), and the ones after it exist only in the WAL. Startup
    /// replays the WAL after the newest snapshot's number, so a snapshot that persists no WAL data
    /// (a compaction's) must record this number, never a later one. `None` until a snapshot exists.
    pub fn last_persisted_wal_sequence_number(&self) -> Option<WalFileSequenceNumber> {
        self.inner.read().last_persisted_wal_sequence_number
    }

    /// Record that a data snapshot covering the WAL up to `wal_file_sequence_number` completed.
    /// The number never moves backwards.
    pub fn set_last_persisted_wal_sequence_number(
        &self,
        wal_file_sequence_number: WalFileSequenceNumber,
    ) {
        let mut inner = self.inner.write();
        let last = inner
            .last_persisted_wal_sequence_number
            .map_or(wal_file_sequence_number, |n| {
                n.max(wal_file_sequence_number)
            });
        inner.last_persisted_wal_sequence_number = Some(last);
    }

    /// Add all files from a persisted snapshot
    pub fn add_persisted_snapshot_files(&self, persisted_snapshot: PersistedSnapshot) {
        let mut inner = self.inner.write();
        inner.add_persisted_snapshot(persisted_snapshot);
    }

    /// Add single file to a table
    pub fn add_persisted_file(&self, db_id: &DbId, table_id: &TableId, parquet_file: &ParquetFile) {
        let mut inner = self.inner.write();
        inner.add_persisted_file(db_id, table_id, parquet_file);
    }

    /// Remove specific files from a table (for compaction and drift pruning).
    ///
    /// Entries are matched by [`ParquetFileId`] and path, as the snapshot loader matches the
    /// `removed_files` it replays: removing a stale entry never drops a different entry that
    /// shares its path, or one that reuses its id (builds before TAG-2495 could reuse ids after a
    /// restart). Metrics are adjusted by the entries actually removed; files that are not in the
    /// index are ignored.
    pub fn remove_persisted_files(
        &self,
        db_id: &DbId,
        table_id: &TableId,
        files_to_remove: &[ParquetFile],
    ) {
        let to_remove: HashSet<(ParquetFileId, &str)> = files_to_remove
            .iter()
            .map(|f| (f.id, f.path.as_str()))
            .collect();
        let mut inner = self.inner.write();
        let Some(files) = inner
            .files
            .get_mut(db_id)
            .and_then(|tables| tables.get_mut(table_id))
        else {
            return;
        };

        let mut removed = Removed::default();
        files.retain(|file| {
            if to_remove.contains(&(file.id, file.path.as_str())) {
                removed.add(file);
                false
            } else {
                true
            }
        });
        inner.subtract(&removed);
    }

    /// The entries of `files` that are in the table's index now with the same id, path and size.
    /// The size matters to the index check: it compared the object against this size, and an
    /// entry replaced meanwhile by one with the same id and path but another size is not the
    /// entry it checked. This guards the decision only: removal (and a restart replaying
    /// `removed_files`) still matches by id and path, like [`Self::remove_persisted_files`].
    pub fn indexed(
        &self,
        db_id: DbId,
        table_id: TableId,
        files: &[ParquetFile],
    ) -> Vec<ParquetFile> {
        let inner = self.inner.read();
        let Some(table_files) = inner.files.get(&db_id).and_then(|t| t.get(&table_id)) else {
            return vec![];
        };
        let indexed: HashSet<(ParquetFileId, &str, u64)> = table_files
            .iter()
            .map(|f| (f.id, f.path.as_str(), f.size_bytes))
            .collect();
        files
            .iter()
            .filter(|f| indexed.contains(&(f.id, f.path.as_str(), f.size_bytes)))
            .cloned()
            .collect()
    }

    /// Get the list of files for a given database and table, always return in descending order of min_time
    pub fn get_files(&self, db_id: DbId, table_id: TableId) -> Vec<ParquetFile> {
        self.get_files_filtered(db_id, table_id, &ChunkFilter::default())
    }

    /// Get the list of files for a given database and table, using the provided filter to filter results.
    ///
    /// Always return in descending order of min_time
    pub fn get_files_filtered(
        &self,
        db_id: DbId,
        table_id: TableId,
        filter: &ChunkFilter<'_>,
    ) -> Vec<ParquetFile> {
        let inner = self.inner.read();
        let mut files = inner
            .files
            .get(&db_id)
            .and_then(|tables| tables.get(&table_id))
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|file| filter.test_time_stamp_min_max(file.min_time, file.max_time))
            .collect::<Vec<_>>();

        files.sort_by(|a, b| b.min_time.cmp(&a.min_time));

        files
    }

    /// Remove the entries of files that are marked for deletion or that violate their retention
    /// period, and return the removed entries by database and table.
    ///
    /// Each due entry is removed by database, table, [`ParquetFileId`] and path, like
    /// [`Self::remove_persisted_files`]. An entry that only shares an id with a due entry stays
    /// indexed (builds before TAG-2495 could reuse ids after a restart). The returned map holds
    /// exactly the entries removed from the index and from the metrics. The caller records them
    /// in the next snapshot's `removed_files` and deletes their objects once that snapshot is
    /// persisted.
    pub fn remove_files_for_deletion(
        &self,
        catalog: Arc<Catalog>,
    ) -> SerdeVecMap<DbId, DatabaseTables> {
        // The scan holds an upgradable read lock, so queries keep running alongside it. Upgrading
        // it to the write lock is atomic: nothing changes the index or the delete marks between
        // the scan and the removal.
        let guard = self.inner.upgradable_read();

        // The id and path of each due entry, by table. A set, so each entry is queued once.
        let mut due: BTreeMap<(DbId, TableId), HashSet<(ParquetFileId, String)>> = BTreeMap::new();
        {
            let mut queue_for_removal = |db_id: DbId, table_id: TableId, file: &ParquetFile| {
                due.entry((db_id, table_id))
                    .or_default()
                    .insert((file.id, file.path.clone()));
            };

            // Remove any data marked for hard-deletion.
            for (db_id, deleted) in guard.deleted_data.iter() {
                let Some(tables) = guard.files.get(db_id) else {
                    continue;
                };

                match deleted {
                    DeletedTables::All => {
                        for (table_id, files) in tables {
                            for file in files {
                                queue_for_removal(*db_id, *table_id, file);
                            }
                        }
                    }
                    DeletedTables::List(table_ids) => {
                        for (table_id, files) in table_ids.iter().filter_map(|table_id| {
                            tables.get(table_id).map(|file| (table_id, file))
                        }) {
                            for file in files {
                                queue_for_removal(*db_id, *table_id, file);
                            }
                        }
                    }
                }
            }

            let retention_periods = catalog.get_retention_period_cutoff_map();

            for ((db_id, table_id), cutoff) in retention_periods {
                // If the database or table is deleted, the files are already scheduled for deletion.
                match guard.deleted_data.get(&db_id) {
                    Some(DeletedTables::All) => {
                        continue;
                    }
                    Some(DeletedTables::List(tables)) if tables.contains(&table_id) => {
                        continue;
                    }
                    _ => {}
                }

                let Some(files) = guard.files.get(&db_id).and_then(|hm| hm.get(&table_id)) else {
                    continue;
                };
                for file in files {
                    // remove files if their max time (aka newest timestamp) is less than (aka older
                    // than) the cutoff timestamp for the retention period
                    if file.max_time < cutoff {
                        queue_for_removal(db_id, table_id, file);
                    }
                }
            }
        }

        // if no persisted files are found to be in violation of their retention period, then
        // return an empty result to avoid unnecessarily acquiring a write lock
        if due.is_empty() {
            return SerdeVecMap::new();
        }

        let mut guard = RwLockUpgradableReadGuard::upgrade(guard);
        // The result is built from the entries this pass removes, so it always describes the same
        // entries as the index and the metrics.
        let mut removed: SerdeVecMap<DbId, DatabaseTables> = SerdeVecMap::new();
        let mut actually_removed = Removed::default();
        for ((db_id, table_id), due_in_table) in &due {
            let Some(files) = guard
                .files
                .get_mut(db_id)
                .and_then(|tables| tables.get_mut(table_id))
            else {
                continue;
            };
            let due_in_table: HashSet<(ParquetFileId, &str)> = due_in_table
                .iter()
                .map(|(id, path)| (*id, path.as_str()))
                .collect();
            let mut removed_from_table = Vec::new();
            files.retain(|file| {
                if due_in_table.contains(&(file.id, file.path.as_str())) {
                    actually_removed.add(file);
                    removed_from_table.push(file.clone());
                    false
                } else {
                    true
                }
            });
            if !removed_from_table.is_empty() {
                removed
                    .entry(*db_id)
                    .or_default()
                    .tables
                    .insert(*table_id, removed_from_table);
            }
        }
        guard.subtract(&actually_removed);

        // The deleted data has been processed.
        guard.deleted_data = HashMap::new();

        removed
    }
}

impl ParquetMetrics for PersistedFiles {
    /// Get parquet file metrics, file count, row count and size in MB
    fn get_metrics(&self) -> (u64, f64, u64) {
        let inner = self.inner.read();
        (
            inner.parquet_files_count,
            inner.parquet_files_size_mb,
            inner.parquet_files_row_count,
        )
    }
}

#[derive(Debug, Default)]
struct Inner {
    /// The map of databases to tables to files
    pub files: DatabaseToTables,
    /// Overall count of the parquet files
    pub parquet_files_count: u64,
    /// Total size of all parquet files in MB
    pub parquet_files_size_mb: f64,
    /// Overall row count within the parquet files
    pub parquet_files_row_count: u64,
    /// Data that are marked for deletion.
    pub deleted_data: HashMap<DbId, DeletedTables>,
    /// See [`PersistedFiles::last_persisted_wal_sequence_number`].
    pub last_persisted_wal_sequence_number: Option<WalFileSequenceNumber>,
}

impl Inner {
    /// Build the index from persisted snapshots, as returned by `Persister::load_snapshots`
    /// (newest snapshot first).
    ///
    /// Snapshots are replayed oldest to newest so that a snapshot's `removed_files` apply to
    /// files added by the older snapshots before it, and so that when the same path is listed
    /// more than once the newest entry wins: object storage holds a single object per path, and
    /// the most recent write is the one that describes it. This collapses same-path duplicates
    /// left behind by earlier versions, so a restart heals them.
    pub(crate) fn new_from_persisted_snapshots(
        persisted_snapshots: Vec<PersistedSnapshot>,
    ) -> Self {
        let mut fold = SnapshotFold::default();
        for persisted_snapshot in persisted_snapshots.into_iter().rev() {
            fold.apply(persisted_snapshot);
        }
        Self::from_fold(fold)
    }

    fn from_fold(fold: SnapshotFold) -> Self {
        let SnapshotFold {
            tables,
            collapsed_duplicates,
            newest,
            ..
        } = fold;
        if collapsed_duplicates > 0 {
            warn!(
                collapsed_duplicates,
                "collapsed parquet index entries sharing a path while loading snapshots; kept the newest entry for each path"
            );
        }

        let mut inner = Self::default();
        let mut size_bytes = 0_u64;
        for ((db_id, table_id), loader) in tables {
            let files = loader.into_files();
            for file in &files {
                inner.parquet_files_count += 1;
                inner.parquet_files_row_count += file.row_count;
                size_bytes += file.size_bytes;
            }
            inner
                .files
                .entry(db_id)
                .or_default()
                .insert(table_id, files);
        }
        inner.parquet_files_size_mb = as_mb(size_bytes);
        // The WAL replay on startup starts after the newest snapshot's number (see
        // `WriteBufferImpl::new`).
        inner.last_persisted_wal_sequence_number = newest.map(|h| h.wal_file_sequence_number);
        inner
    }

    /// Add all files from a persisted snapshot and apply its removals.
    ///
    /// Files are added with [`Self::add_persisted_file`] semantics (a file whose path is already
    /// indexed replaces the existing entry). Removals match by [`ParquetFileId`] and path, like
    /// the snapshot loader's: an entry that only shares an id with a removed file stays.
    pub(crate) fn add_persisted_snapshot(&mut self, persisted_snapshot: PersistedSnapshot) {
        let mut added = Removed::default();
        let mut replaced = Removed::default();
        for (db_id, db_tables) in persisted_snapshot.databases {
            for (table_id, files) in db_tables.tables {
                for file in &files {
                    self.upsert_file(&db_id, &table_id, file, &mut replaced);
                    added.add(file);
                }
            }
        }
        self.parquet_files_count += added.count;
        self.parquet_files_row_count += added.rows;
        self.parquet_files_size_mb += as_mb(added.size_bytes);
        self.subtract(&replaced);
        for (db_id, db_tables) in persisted_snapshot.removed_files {
            for (table_id, files) in db_tables.tables {
                let Some(table_files) = self
                    .files
                    .get_mut(&db_id)
                    .and_then(|tables| tables.get_mut(&table_id))
                else {
                    continue;
                };
                let to_remove: HashSet<(ParquetFileId, &str)> =
                    files.iter().map(|f| (f.id, f.path.as_str())).collect();
                let mut removed = Removed::default();
                table_files.retain(|file| {
                    if to_remove.contains(&(file.id, file.path.as_str())) {
                        removed.add(file);
                        false
                    } else {
                        true
                    }
                });
                self.subtract(&removed);
            }
        }
    }

    /// Add a single file to a table's index.
    ///
    /// Object storage holds exactly one object per path, so the index keeps exactly one entry
    /// per path: adding a file whose path is already indexed replaces the existing entry (the
    /// newest write describes what is in object storage now). Re-adding an identical entry is
    /// a no-op for the metrics.
    pub(crate) fn add_persisted_file(
        &mut self,
        db_id: &DbId,
        table_id: &TableId,
        parquet_file: &ParquetFile,
    ) {
        let mut replaced = Removed::default();
        self.upsert_file(db_id, table_id, parquet_file, &mut replaced);
        self.subtract(&replaced);
        self.parquet_files_count += 1;
        self.parquet_files_row_count += parquet_file.row_count;
        self.parquet_files_size_mb += as_mb(parquet_file.size_bytes);
    }

    /// Insert `parquet_file` into its table, removing any entry with the same path. Removed
    /// entries are accumulated into `replaced`; metrics are left to the caller.
    fn upsert_file(
        &mut self,
        db_id: &DbId,
        table_id: &TableId,
        parquet_file: &ParquetFile,
        replaced: &mut Removed,
    ) {
        let table_files = self
            .files
            .entry(*db_id)
            .or_default()
            .entry(*table_id)
            .or_default();
        table_files.retain(|file| {
            if file.path == parquet_file.path {
                if file != parquet_file {
                    warn!(
                        path = %file.path,
                        old_id = file.id.as_u64(),
                        old_size_bytes = file.size_bytes,
                        new_id = parquet_file.id.as_u64(),
                        new_size_bytes = parquet_file.size_bytes,
                        "replacing parquet index entry for a path that is already indexed"
                    );
                }
                replaced.add(file);
                false
            } else {
                true
            }
        });
        table_files.push(parquet_file.clone());
    }

    /// Subtract removed entries from the metrics.
    fn subtract(&mut self, removed: &Removed) {
        self.parquet_files_count = self.parquet_files_count.saturating_sub(removed.count);
        self.parquet_files_row_count = self.parquet_files_row_count.saturating_sub(removed.rows);
        self.parquet_files_size_mb -= as_mb(removed.size_bytes);
    }
}

/// Running totals of index entries removed from the index.
#[derive(Debug, Default)]
struct Removed {
    count: u64,
    size_bytes: u64,
    rows: u64,
}

impl Removed {
    fn add(&mut self, file: &ParquetFile) {
        self.count += 1;
        self.size_bytes += file.size_bytes;
        self.rows += file.row_count;
    }
}

/// The fields of the newest snapshot a [`SnapshotFold`] applied. Startup takes the WAL replay
/// start, the snapshot sequence number and the next parquet file id from them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SnapshotHeader {
    pub(crate) snapshot_sequence_number: SnapshotSequenceNumber,
    pub(crate) wal_file_sequence_number: WalFileSequenceNumber,
    pub(crate) catalog_sequence_number: CatalogSequenceNumber,
    pub(crate) next_file_id: ParquetFileId,
}

/// Builds the parquet index from persisted snapshots applied one at a time, oldest to newest.
/// The startup loader and the index checkpoint writer both use it, so a checkpoint holds exactly
/// what a restart that loads the same snapshots would index.
///
/// A snapshot's files are added (an entry whose path is already indexed is replaced: the newest
/// write describes the object), then its `removed_files` are removed by id and path, so a removal
/// only applies to files added by older snapshots and an entry that only shares an id stays.
#[derive(Debug, Default)]
pub(crate) struct SnapshotFold {
    tables: HashMap<(DbId, TableId), TableLoader>,
    collapsed_duplicates: u64,
    newest: Option<SnapshotHeader>,
    applied: usize,
}

impl SnapshotFold {
    /// Apply `snapshot`, which must be newer than every snapshot applied before it.
    pub(crate) fn apply(&mut self, snapshot: PersistedSnapshot) {
        self.newest = Some(SnapshotHeader {
            snapshot_sequence_number: snapshot.snapshot_sequence_number,
            wal_file_sequence_number: snapshot.wal_file_sequence_number,
            catalog_sequence_number: snapshot.catalog_sequence_number,
            next_file_id: snapshot.next_file_id,
        });
        self.applied += 1;
        for (db_id, db_tables) in snapshot.databases {
            for (table_id, files) in db_tables.tables {
                let loader = self.tables.entry((db_id, table_id)).or_default();
                for file in files {
                    if loader.upsert(file) {
                        self.collapsed_duplicates += 1;
                    }
                }
            }
        }
        for (db_id, db_tables) in snapshot.removed_files {
            for (table_id, files) in db_tables.tables {
                if let Some(loader) = self.tables.get_mut(&(db_id, table_id)) {
                    for file in &files {
                        loader.remove(file);
                    }
                }
            }
        }
    }

    /// Whether the table indexes an entry at `path`.
    pub(crate) fn indexes(&self, db_id: DbId, table_id: TableId, path: &str) -> bool {
        self.tables
            .get(&(db_id, table_id))
            .is_some_and(|loader| loader.by_path.contains_key(path))
    }

    /// The newest snapshot applied so far, if any.
    pub(crate) fn newest(&self) -> Option<SnapshotHeader> {
        self.newest
    }

    /// How many snapshots have been applied.
    pub(crate) fn applied(&self) -> usize {
        self.applied
    }

    /// Every indexed file, by database and table, in the order the fold first indexed each
    /// path. Tables whose files were all removed are left out.
    pub(crate) fn files(&self) -> BTreeMap<(DbId, TableId), Vec<ParquetFile>> {
        self.tables
            .iter()
            .map(|(key, loader)| (*key, loader.files().cloned().collect::<Vec<_>>()))
            .filter(|(_, files)| !files.is_empty())
            .collect()
    }

    /// Drop the slots of removed entries and the tables left empty. A fold kept across many
    /// snapshots (the checkpoint writer's) would otherwise grow with every entry ever removed.
    pub(crate) fn compact(&mut self) {
        self.tables.retain(|_, loader| {
            loader.compact();
            !loader.files.is_empty()
        });
    }

    /// Like [`Self::files`], consuming the fold.
    #[cfg(test)]
    pub(crate) fn into_files(self) -> BTreeMap<(DbId, TableId), Vec<ParquetFile>> {
        self.tables
            .into_iter()
            .map(|(key, loader)| (key, loader.into_files()))
            .filter(|(_, files)| !files.is_empty())
            .collect()
    }
}

/// Per-table state while loading snapshots: keeps one entry per path in insertion order, with a
/// path lookup so loading many snapshots stays linear.
#[derive(Debug, Default)]
struct TableLoader {
    files: Vec<Option<ParquetFile>>,
    by_path: HashMap<String, usize>,
}

impl TableLoader {
    /// Insert `file`, replacing any entry with the same path. Returns true if an entry with a
    /// different id was replaced.
    fn upsert(&mut self, file: ParquetFile) -> bool {
        match self.by_path.get(&file.path) {
            Some(&idx) => {
                let slot = &mut self.files[idx];
                let collapsed = slot.as_ref().is_some_and(|old| old.id != file.id);
                *slot = Some(file);
                collapsed
            }
            None => {
                self.by_path.insert(file.path.clone(), self.files.len());
                self.files.push(Some(file));
                false
            }
        }
    }

    /// Remove the entry for `file`'s path if it is the same entry (same id). An entry that has
    /// already been superseded by a newer write to that path is left alone.
    fn remove(&mut self, file: &ParquetFile) {
        let Some(&idx) = self.by_path.get(&file.path) else {
            return;
        };
        if self.files[idx].as_ref().is_some_and(|f| f.id == file.id) {
            self.files[idx] = None;
            self.by_path.remove(&file.path);
        }
    }

    fn into_files(self) -> Vec<ParquetFile> {
        self.files.into_iter().flatten().collect()
    }

    fn files(&self) -> impl Iterator<Item = &ParquetFile> {
        self.files.iter().flatten()
    }

    /// Drop the empty slots that removals leave, keeping the order.
    fn compact(&mut self) {
        self.files.retain(Option::is_some);
        self.by_path = self
            .files
            .iter()
            .enumerate()
            .map(|(i, file)| (file.as_ref().expect("retained").path.clone(), i))
            .collect();
    }
}

fn as_mb(bytes: u64) -> f64 {
    let factor = (1_000 * 1_000) as f64;
    bytes as f64 / factor
}

#[cfg(test)]
mod tests {

    use crate::ParquetFileId;
    use datafusion::prelude::Expr;
    use datafusion::prelude::col;
    use datafusion::prelude::lit_timestamp_nano;
    use influxdb3_catalog::catalog::CatalogArgs;
    use influxdb3_catalog::catalog::CatalogSequenceNumber;
    use influxdb3_catalog::catalog::TableDefinition;
    use influxdb3_catalog::log::FieldDataType;
    use influxdb3_id::ColumnId;
    use influxdb3_wal::{SnapshotSequenceNumber, WalFileSequenceNumber};
    use iox_time::{MockProvider, Time};
    use observability_deps::tracing::info;
    use pretty_assertions::assert_eq;
    use schema::InfluxColumnType;
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;

    #[test_log::test(test)]
    fn test_get_metrics_after_initial_load() {
        let all_persisted_snapshot_files = build_persisted_snapshots();
        let persisted_file =
            PersistedFiles::new_from_persisted_snapshots(all_persisted_snapshot_files);

        let (file_count, size_in_mb, row_count) = persisted_file.get_metrics();

        info!(metrics = ?persisted_file.get_metrics(), "All files metrics");
        assert_eq!(10, file_count);
        assert_eq!(0.5, size_in_mb);
        assert_eq!(100, row_count);
    }

    #[test_log::test(test)]
    fn test_get_metrics_after_update() {
        let all_persisted_snapshot_files = build_persisted_snapshots();
        let persisted_file =
            PersistedFiles::new_from_persisted_snapshots(all_persisted_snapshot_files);
        let parquet_files = build_parquet_files("update_", 5);
        let new_snapshot = build_snapshot(parquet_files, 1, 1, 1);
        persisted_file.add_persisted_snapshot_files(new_snapshot);

        let (file_count, size_in_mb, row_count) = persisted_file.get_metrics();

        info!(metrics = ?persisted_file.get_metrics(), "All files metrics");
        assert_eq!(15, file_count);
        assert_eq!(0.75, size_in_mb);
        assert_eq!(150, row_count);
    }

    #[test_log::test(test)]
    fn test_get_metrics_after_update_with_duplicate_file() {
        let all_persisted_snapshot_files = build_persisted_snapshots();
        let already_existing_file = all_persisted_snapshot_files
            .last()
            .unwrap()
            .databases
            .get(&DbId::from(0))
            .unwrap()
            .tables
            .get(&TableId::from(0))
            .unwrap()
            .last()
            .cloned()
            .unwrap();

        let persisted_file =
            PersistedFiles::new_from_persisted_snapshots(all_persisted_snapshot_files);
        let mut parquet_files = build_parquet_files("update_", 4);
        info!(all_persisted_files = ?persisted_file, "Full persisted file");
        info!(already_existing_file = ?already_existing_file, "Existing file");
        parquet_files.push(already_existing_file);

        let new_snapshot = build_snapshot(parquet_files, 1, 1, 1);
        persisted_file.add_persisted_snapshot_files(new_snapshot);

        let (file_count, size_in_mb, row_count) = persisted_file.get_metrics();
        info!(all_persisted_files = ?persisted_file, "Full persisted file after");

        info!(metrics = ?persisted_file.get_metrics(), "All files metrics");
        assert_eq!(14, file_count);
        // Metrics count the files actually in the index; re-adding an already indexed file
        // does not count it twice.
        assert!(
            (size_in_mb - 0.70).abs() < 1e-9,
            "size_in_mb = {size_in_mb}"
        );
        assert_eq!(140, row_count);
    }

    #[test]
    fn test_get_files_with_filters() {
        let parquet_files = (0..100)
            .step_by(10)
            .map(|i| {
                let chunk_time = i;
                ParquetFile {
                    id: ParquetFileId::new(),
                    path: format!("/path/{i:03}.parquet"),
                    size_bytes: 1,
                    row_count: 1,
                    chunk_time,
                    min_time: chunk_time,
                    max_time: chunk_time + 10,
                }
            })
            .collect();
        let persisted_snapshots = vec![build_snapshot(parquet_files, 0, 0, 0)];
        let persisted_files = PersistedFiles::new_from_persisted_snapshots(persisted_snapshots);

        struct TestCase<'a> {
            filter: &'a [Expr],
            expected_n_files: usize,
        }

        let test_cases = [
            TestCase {
                filter: &[],
                expected_n_files: 10,
            },
            TestCase {
                filter: &[col("time").gt(lit_timestamp_nano(0))],
                expected_n_files: 10,
            },
            TestCase {
                filter: &[col("time").gt(lit_timestamp_nano(50))],
                expected_n_files: 5,
            },
            TestCase {
                filter: &[col("time").gt(lit_timestamp_nano(90))],
                expected_n_files: 1,
            },
            TestCase {
                filter: &[col("time").gt(lit_timestamp_nano(100))],
                expected_n_files: 0,
            },
            TestCase {
                filter: &[col("time").lt(lit_timestamp_nano(100))],
                expected_n_files: 10,
            },
            TestCase {
                filter: &[col("time").lt(lit_timestamp_nano(50))],
                expected_n_files: 5,
            },
            TestCase {
                filter: &[col("time").lt(lit_timestamp_nano(10))],
                expected_n_files: 1,
            },
            TestCase {
                filter: &[col("time").lt(lit_timestamp_nano(0))],
                expected_n_files: 0,
            },
            TestCase {
                filter: &[col("time")
                    .gt(lit_timestamp_nano(20))
                    .and(col("time").lt(lit_timestamp_nano(40)))],
                expected_n_files: 2,
            },
            TestCase {
                filter: &[col("time")
                    .gt(lit_timestamp_nano(20))
                    .and(col("time").lt(lit_timestamp_nano(30)))],
                expected_n_files: 1,
            },
            TestCase {
                filter: &[col("time")
                    .gt(lit_timestamp_nano(21))
                    .and(col("time").lt(lit_timestamp_nano(29)))],
                expected_n_files: 1,
            },
            TestCase {
                filter: &[col("time")
                    .gt(lit_timestamp_nano(0))
                    .and(col("time").lt(lit_timestamp_nano(100)))],
                expected_n_files: 10,
            },
        ];

        let table_def = Arc::new(
            TableDefinition::new(
                TableId::from(0),
                "test-tbl".into(),
                vec![(
                    ColumnId::from(0),
                    "time".into(),
                    InfluxColumnType::Timestamp,
                )],
                vec![],
            )
            .unwrap(),
        );

        for t in test_cases {
            let filter = ChunkFilter::new(&table_def, t.filter).unwrap();
            let filtered_files =
                persisted_files.get_files_filtered(DbId::from(0), TableId::from(0), &filter);
            assert_eq!(
                t.expected_n_files,
                filtered_files.len(),
                "wrong number of filtered files:\n\
                result: {filtered_files:?}\n\
                filter provided: {filter:?}"
            );
        }
    }

    fn build_persisted_snapshots() -> Vec<PersistedSnapshot> {
        let mut all_persisted_snapshot_files = Vec::new();
        let parquet_files_1 = build_parquet_files("snap1_", 5);
        all_persisted_snapshot_files.push(build_snapshot(parquet_files_1, 1, 1, 1));

        let parquet_files_2 = build_parquet_files("snap2_", 5);
        all_persisted_snapshot_files.push(build_snapshot(parquet_files_2, 2, 2, 2));

        all_persisted_snapshot_files
    }

    fn build_snapshot(
        parquet_files: Vec<ParquetFile>,
        snapshot_id: u64,
        wal_id: u64,
        catalog_id: u64,
    ) -> PersistedSnapshot {
        let snap1 = SnapshotSequenceNumber::new(snapshot_id);
        let wal1 = WalFileSequenceNumber::new(wal_id);
        let cat1 = CatalogSequenceNumber::new(catalog_id);
        let mut new_snapshot =
            PersistedSnapshot::new("sample-host-id".to_owned(), snap1, wal1, cat1);
        parquet_files.into_iter().for_each(|file| {
            // TODO: Check why `add_parquet_file` method does not check if file is
            //       already present. This is checked when trying to add a new PersistedSnapshot
            //       as part of snapshotting process.
            new_snapshot.add_parquet_file(DbId::from(0), TableId::from(0), file);
        });
        new_snapshot
    }

    fn build_parquet_files(prefix: &str, num_files: u32) -> Vec<ParquetFile> {
        let parquet_files: Vec<ParquetFile> = (0..num_files)
            .map(|i| ParquetFile {
                id: ParquetFileId::new(),
                path: format!("/random/path/{prefix}_{i}.parquet"),
                size_bytes: 50_000,
                row_count: 10,
                chunk_time: 10,
                min_time: 10,
                max_time: 200,
            })
            .collect();
        parquet_files
    }

    #[tokio::test]
    async fn test_remove_files_for_deletion_deleted_database() {
        let parquet_files = build_parquet_files("file_", 5);
        let mut snapshot = build_snapshot(parquet_files.clone(), 1, 1, 1);

        // Add files to a second database
        let db_id2 = DbId::from(1);
        let table_id2 = TableId::from(1);
        let more_files = build_parquet_files("db2_", 3);
        for file in more_files.iter() {
            snapshot.add_parquet_file(db_id2, table_id2, file.clone());
        }

        let persisted_files = PersistedFiles::new_from_persisted_snapshots(vec![snapshot]);

        // Delete the first database
        persisted_files.delete_database(DbId::from(0));

        // Create a mock catalog that returns no retention periods
        let catalog = Arc::new(Catalog::new_in_memory("test").await.unwrap());

        let removed = persisted_files.remove_files_for_deletion(catalog);

        // Verify all files from database 0 are removed
        assert_eq!(removed.len(), 1);
        let db_tables = removed.get(&DbId::from(0)).unwrap();
        assert_eq!(db_tables.tables.len(), 1);
        let table_files = db_tables.tables.get(&TableId::from(0)).unwrap();
        assert_eq!(table_files.len(), 5);

        // Verify database 1 files remain
        let remaining_files = persisted_files.get_files(db_id2, table_id2);
        assert_eq!(remaining_files.len(), 3);

        // Verify metrics are updated
        let (file_count, size_mb, row_count) = persisted_files.get_metrics();
        assert_eq!(file_count, 3);
        assert!((size_mb - 0.15).abs() < 0.0001); // 3 files * 50_000 bytes (check with epsilon for floating point)
        assert_eq!(row_count, 30); // 3 files * 10 rows
    }

    /// Files removed at runtime (hard delete or retention) are recorded in the next snapshot's
    /// `removed_files`, as `QueryableBuffer` does. After a restart they must stay removed, even
    /// though an older snapshot still lists them. Snapshots used to be folded newest first, so
    /// the older snapshot's adds were applied after the removal and the files came back.
    #[tokio::test]
    async fn test_files_removed_for_deletion_stay_removed_after_restart() {
        let db_id = DbId::from(0);
        let kept_table = TableId::from(0);
        let deleted_table = TableId::from(1);

        let kept_files = build_parquet_files("kept_", 3);
        let mut snapshot_1 = build_snapshot(kept_files.clone(), 1, 1, 1);
        for file in build_parquet_files("deleted_", 2) {
            snapshot_1.add_parquet_file(db_id, deleted_table, file);
        }

        let persisted_files =
            PersistedFiles::new_from_persisted_snapshots(vec![snapshot_1.clone()]);
        persisted_files.delete_table(db_id, deleted_table);
        let catalog = Arc::new(Catalog::new_in_memory("test").await.unwrap());
        let removed = persisted_files.remove_files_for_deletion(catalog);
        assert_eq!(removed.get(&db_id).unwrap().tables[&deleted_table].len(), 2);

        // The next snapshot carries a new file and the runtime removals.
        let new_file = build_parquet_files("new_", 1);
        let mut snapshot_2 = build_snapshot(new_file.clone(), 2, 2, 2);
        snapshot_2.removed_files = removed;

        // Restart: `Persister::load_snapshots` returns the newest snapshot first.
        let reloaded = PersistedFiles::new_from_persisted_snapshots(vec![snapshot_2, snapshot_1]);

        assert!(reloaded.get_files(db_id, deleted_table).is_empty());
        let mut kept: Vec<_> = reloaded
            .get_files(db_id, kept_table)
            .into_iter()
            .map(|f| f.id)
            .collect();
        kept.sort();
        let mut expected: Vec<_> = kept_files.iter().chain(&new_file).map(|f| f.id).collect();
        expected.sort();
        assert_eq!(kept, expected);

        let (file_count, size_mb, row_count) = reloaded.get_metrics();
        assert_eq!(file_count, 4);
        assert_eq!(size_mb, 0.2); // 4 files * 50_000 bytes
        assert_eq!(row_count, 40); // 4 files * 10 rows
    }

    #[tokio::test]
    async fn test_remove_files_for_deletion_deleted_tables() {
        let parquet_files = build_parquet_files("file_", 3);
        let mut snapshot = build_snapshot(parquet_files.clone(), 1, 1, 1);

        // Add files to multiple tables in the same database
        let db_id = DbId::from(0);
        let table_id1 = TableId::from(0);
        let table_id2 = TableId::from(1);
        let table_id3 = TableId::from(2);

        let table2_files = build_parquet_files("table2_", 4);
        for file in table2_files.iter() {
            snapshot.add_parquet_file(db_id, table_id2, file.clone());
        }

        let table3_files: Vec<ParquetFile> = build_parquet_files("table_3", 2);
        for file in table3_files.iter() {
            snapshot.add_parquet_file(db_id, table_id3, file.clone());
        }

        let persisted_files = PersistedFiles::new_from_persisted_snapshots(vec![snapshot]);

        // Delete specific tables
        persisted_files.delete_table(db_id, table_id1);
        persisted_files.delete_table(db_id, table_id3);

        let catalog = Arc::new(Catalog::new_in_memory("test").await.unwrap());

        let removed = persisted_files.remove_files_for_deletion(catalog);

        // Verify only tables 1 and 3 are removed
        assert_eq!(removed.len(), 1);
        let db_tables = removed.get(&db_id).unwrap();
        assert_eq!(db_tables.tables.len(), 2);

        let table1_removed = db_tables.tables.get(&table_id1).unwrap();
        assert_eq!(table1_removed.len(), 3);

        let table3_removed = db_tables.tables.get(&table_id3).unwrap();
        assert_eq!(table3_removed.len(), 2);

        // Verify table 2 remains
        let table2_remaining = persisted_files.get_files(db_id, table_id2);
        assert_eq!(table2_remaining.len(), 4);

        // Verify metrics
        let (file_count, size_mb, row_count) = persisted_files.get_metrics();
        assert_eq!(file_count, 4);
        assert_eq!(size_mb, 0.2); // 4 files * 50_000 bytes
        assert_eq!(row_count, 40); // 4 files * 10 rows
    }

    #[tokio::test]
    async fn test_remove_files_for_deletion_clears_deleted_data() {
        let parquet_files = build_parquet_files("file_", 3);
        let snapshot = build_snapshot(parquet_files.clone(), 1, 1, 1);
        let persisted_files = PersistedFiles::new_from_persisted_snapshots(vec![snapshot]);

        // Delete a database
        persisted_files.delete_database(DbId::from(0));

        // Verify deleted_data is populated
        {
            let inner = persisted_files.inner.read();
            assert_eq!(inner.deleted_data.len(), 1);
        }

        let catalog = Arc::new(Catalog::new_in_memory("test").await.unwrap());

        persisted_files.remove_files_for_deletion(catalog);

        // Verify deleted_data is cleared after removal
        {
            let inner = persisted_files.inner.read();
            assert_eq!(inner.deleted_data.len(), 0);
        }
    }

    fn parquet_file(id: u64, path: &str, size_bytes: u64, row_count: u64) -> ParquetFile {
        ParquetFile {
            id: ParquetFileId::from(id),
            path: path.to_string(),
            size_bytes,
            row_count,
            chunk_time: 10,
            min_time: 10,
            max_time: 200,
        }
    }

    fn assert_metrics(persisted_files: &PersistedFiles, count: u64, size_bytes: u64, rows: u64) {
        let (file_count, size_mb, row_count) = persisted_files.get_metrics();
        assert_eq!(file_count, count, "file count");
        assert!(
            (size_mb - as_mb(size_bytes)).abs() < 1e-9,
            "size_mb = {size_mb}, expected {}",
            as_mb(size_bytes)
        );
        assert_eq!(row_count, rows, "row count");
    }

    #[test]
    fn remove_persisted_files_matches_by_id_not_path() {
        let db_id = DbId::from(0);
        let table_id = TableId::from(0);
        let stale = parquet_file(1, "gen2/2026-09-18/00-00/0.parquet", 6_730, 10);
        let valid = parquet_file(2, "gen2/2026-09-18/00-00/0.parquet", 6_750, 12);
        let other = parquet_file(3, "gen2/2026-09-18/01-00/0.parquet", 1_000, 5);

        // Reproduce the damaged state (two entries for one path) directly in the index; the
        // public add path no longer allows it.
        let persisted_files = PersistedFiles::new();
        {
            let mut inner = persisted_files.inner.write();
            inner
                .files
                .entry(db_id)
                .or_default()
                .insert(table_id, vec![stale.clone(), valid.clone(), other.clone()]);
            inner.parquet_files_count = 3;
            inner.parquet_files_row_count = 27;
            inner.parquet_files_size_mb = as_mb(6_730 + 6_750 + 1_000);
        }

        persisted_files.remove_persisted_files(&db_id, &table_id, std::slice::from_ref(&stale));

        let mut remaining = persisted_files.get_files(db_id, table_id);
        remaining.sort_by_key(|f| f.id);
        assert_eq!(remaining, vec![valid, other]);
        assert_metrics(&persisted_files, 2, 6_750 + 1_000, 17);

        // Removing a file that is not indexed is a no-op, including for the metrics.
        persisted_files.remove_persisted_files(&db_id, &table_id, &[stale]);
        assert_eq!(persisted_files.get_files(db_id, table_id).len(), 2);
        assert_metrics(&persisted_files, 2, 6_750 + 1_000, 17);
    }

    /// Builds before TAG-2495 could reuse file ids after a restart, so two entries can share an
    /// id. The snapshot loader removes an entry only when both id and path match; removing from
    /// the running index must do the same, or the index drops an entry that a restart brings back.
    #[test]
    fn remove_persisted_files_keeps_an_entry_that_only_shares_the_id() {
        let db_id = DbId::from(0);
        let table_id = TableId::from(0);
        let stale = parquet_file(7, "gen1/2026-07-10/23-10/0000021161.parquet", 1_000, 10);
        let reused_id = parquet_file(7, "gen1/2026-07-11/00-00/0000021200.parquet", 2_000, 20);

        let persisted_files = PersistedFiles::new();
        persisted_files.add_persisted_file(&db_id, &table_id, &stale);
        persisted_files.add_persisted_file(&db_id, &table_id, &reused_id);

        persisted_files.remove_persisted_files(&db_id, &table_id, std::slice::from_ref(&stale));

        assert_eq!(
            persisted_files.get_files(db_id, table_id),
            vec![reused_id.clone()]
        );
        assert_metrics(&persisted_files, 1, 2_000, 20);

        // The same removal recorded in a snapshot and reloaded gives the same index.
        let snapshot_1 = build_snapshot(vec![stale.clone(), reused_id], 1, 1, 1);
        let mut snapshot_2 = build_snapshot(vec![], 2, 1, 2);
        snapshot_2
            .removed_files
            .entry(db_id)
            .or_default()
            .tables
            .insert(table_id, vec![stale]);
        let reloaded = PersistedFiles::new_from_persisted_snapshots(vec![snapshot_2, snapshot_1]);
        assert_eq!(
            reloaded.get_files(db_id, table_id),
            persisted_files.get_files(db_id, table_id)
        );
    }

    /// Applying a snapshot's `removed_files` to the running index matches by id and path, as the
    /// snapshot loader does: an entry that only shares the id with a removed file (builds before
    /// TAG-2495 could reuse ids after a restart) stays indexed.
    #[test]
    fn add_persisted_snapshot_keeps_an_entry_that_only_shares_the_id() {
        let db_id = DbId::from(0);
        let table_id = TableId::from(0);
        let stale = parquet_file(7, "gen1/2026-07-10/23-10/0000021161.parquet", 1_000, 10);
        let reused_id = parquet_file(7, "gen1/2026-07-11/00-00/0000021200.parquet", 2_000, 20);

        let snapshot_1 = build_snapshot(vec![stale.clone(), reused_id.clone()], 1, 1, 1);
        let persisted_files =
            PersistedFiles::new_from_persisted_snapshots(vec![snapshot_1.clone()]);

        let mut snapshot_2 = build_snapshot(vec![], 2, 2, 2);
        snapshot_2
            .removed_files
            .entry(db_id)
            .or_default()
            .tables
            .insert(table_id, vec![stale]);
        persisted_files.add_persisted_snapshot_files(snapshot_2.clone());

        assert_eq!(
            persisted_files.get_files(db_id, table_id),
            vec![reused_id.clone()]
        );
        assert_metrics(&persisted_files, 1, 2_000, 20);

        // Loading the same snapshots after a restart gives the same index.
        let reloaded = PersistedFiles::new_from_persisted_snapshots(vec![snapshot_2, snapshot_1]);
        assert_eq!(reloaded.get_files(db_id, table_id), vec![reused_id]);
    }

    /// The retention tests' clock: one day after the epoch, in nanoseconds.
    const RETENTION_TEST_NOW_NANOS: i64 = 24 * 60 * 60 * 1_000_000_000;

    /// A catalog whose clock reads [`RETENTION_TEST_NOW_NANOS`], so a one-hour retention period
    /// puts the cutoff long after the `max_time` (200ns) of the entries `parquet_file` builds.
    async fn catalog_for_retention() -> Arc<Catalog> {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(
            RETENTION_TEST_NOW_NANOS,
        )));
        Arc::new(
            Catalog::new_in_memory_with_args("test", time_provider as _, CatalogArgs::default())
                .await
                .unwrap(),
        )
    }

    /// Create `table` in `db` (and `db` if it does not exist) and return their ids.
    async fn create_table(catalog: &Catalog, db: &str, table: &str) -> (DbId, TableId) {
        if catalog.db_name_to_id(db).is_none() {
            catalog.create_database(db).await.unwrap();
        }
        catalog
            .create_table(db, table, &["tag"], &[("field", FieldDataType::String)])
            .await
            .unwrap();
        let db_schema = catalog.db_schema(db).unwrap();
        (db_schema.id, db_schema.table_name_to_id(table).unwrap())
    }

    /// A snapshot holding each entry in its own database and table.
    fn snapshot_with(entries: &[(DbId, TableId, &ParquetFile)]) -> PersistedSnapshot {
        let mut snapshot = build_snapshot(vec![], 1, 1, 1);
        for (db_id, table_id, file) in entries {
            snapshot.add_parquet_file(*db_id, *table_id, (*file).clone());
        }
        snapshot
    }

    /// Restart after a removal: record `removed` in the next snapshot's `removed_files`, as
    /// `QueryableBuffer` does, and load the index from it and `older` (newest first, as
    /// `Persister::load_snapshots` returns them).
    fn reload_after_removal(
        older: PersistedSnapshot,
        removed: SerdeVecMap<DbId, DatabaseTables>,
    ) -> PersistedFiles {
        let mut newer = build_snapshot(vec![], 2, 2, 2);
        newer.removed_files = removed;
        PersistedFiles::new_from_persisted_snapshots(vec![newer, older])
    }

    /// Builds before TAG-2495 could reuse file ids after a restart, so an index loaded from older
    /// snapshots can hold entries in different tables, or databases, that share an id. Retention
    /// removes only the entry past its retention period. The others stay indexed, and the returned
    /// map, which the caller records in the snapshot's `removed_files` and deletes from object
    /// storage, holds only the removed entry. A restart gives the same index.
    #[tokio::test]
    async fn retention_keeps_entries_that_only_share_the_id() {
        let catalog = catalog_for_retention().await;
        let (retained_db, expired_table) = create_table(&catalog, "retained", "expired").await;
        let (_, recent_table) = create_table(&catalog, "retained", "recent").await;
        let (other_db, other_table) = create_table(&catalog, "no_retention", "cpu").await;
        catalog
            .set_retention_period_for_database("retained", Duration::from_secs(60 * 60))
            .await
            .unwrap();
        let cutoffs = catalog.get_retention_period_cutoff_map();
        assert!(cutoffs.contains_key(&(retained_db, expired_table)));
        assert!(cutoffs.contains_key(&(retained_db, recent_table)));
        assert!(!cutoffs.contains_key(&(other_db, other_table)));

        // Past the retention period (`max_time` is 200ns after the epoch).
        let expired = parquet_file(7, "retained/expired/old.parquet", 1_000, 10);
        // The same id, within the retention period: in the same table at another path, and in
        // another table of the same database.
        let same_table = ParquetFile {
            max_time: RETENTION_TEST_NOW_NANOS,
            ..parquet_file(7, "retained/expired/new.parquet", 2_000, 20)
        };
        let other_table_entry = ParquetFile {
            max_time: RETENTION_TEST_NOW_NANOS,
            ..parquet_file(7, "retained/recent/new.parquet", 4_000, 40)
        };
        // The same id, in a database without a retention period.
        let other_db_entry = parquet_file(7, "no_retention/cpu/old.parquet", 8_000, 80);

        let snapshot = snapshot_with(&[
            (retained_db, expired_table, &expired),
            (retained_db, expired_table, &same_table),
            (retained_db, recent_table, &other_table_entry),
            (other_db, other_table, &other_db_entry),
        ]);
        let persisted_files = PersistedFiles::new_from_persisted_snapshots(vec![snapshot.clone()]);

        let removed = persisted_files.remove_files_for_deletion(catalog);

        assert_eq!(
            persisted_files.get_files(retained_db, expired_table),
            vec![same_table]
        );
        assert_eq!(
            persisted_files.get_files(retained_db, recent_table),
            vec![other_table_entry]
        );
        assert_eq!(
            persisted_files.get_files(other_db, other_table),
            vec![other_db_entry]
        );
        assert_metrics(&persisted_files, 3, 2_000 + 4_000 + 8_000, 20 + 40 + 80);

        assert_eq!(removed.len(), 1);
        let removed_tables = &removed.get(&retained_db).unwrap().tables;
        assert_eq!(removed_tables.len(), 1);
        assert_eq!(removed_tables.get(&expired_table).unwrap(), &vec![expired]);

        let reloaded = reload_after_removal(snapshot, removed);
        for (db_id, table_id) in [
            (retained_db, expired_table),
            (retained_db, recent_table),
            (other_db, other_table),
        ] {
            assert_eq!(
                reloaded.get_files(db_id, table_id),
                persisted_files.get_files(db_id, table_id)
            );
        }
        assert_metrics(&reloaded, 3, 2_000 + 4_000 + 8_000, 20 + 40 + 80);
    }

    /// When several entries that share an id are all due, each one leaves the index and each one
    /// is in the returned map, whether a hard delete or retention made it due. A restart gives
    /// the same index.
    #[tokio::test]
    async fn retention_and_hard_delete_remove_every_due_entry_that_shares_the_id() {
        let catalog = catalog_for_retention().await;
        let (retained_db, table_a) = create_table(&catalog, "retained", "a").await;
        let (_, table_b) = create_table(&catalog, "retained", "b").await;
        let (other_db, deleted_table) = create_table(&catalog, "no_retention", "deleted").await;
        let (_, kept_table) = create_table(&catalog, "no_retention", "kept").await;
        catalog
            .set_retention_period_for_database("retained", Duration::from_secs(60 * 60))
            .await
            .unwrap();
        let cutoffs = catalog.get_retention_period_cutoff_map();
        assert!(cutoffs.contains_key(&(retained_db, table_a)));
        assert!(cutoffs.contains_key(&(retained_db, table_b)));
        assert!(!cutoffs.contains_key(&(other_db, kept_table)));

        // All share id 7 and are older than the retention cutoff.
        let expired_a = parquet_file(7, "retained/a/old.parquet", 1_000, 10);
        let expired_b = parquet_file(7, "retained/b/old.parquet", 2_000, 20);
        let deleted = parquet_file(7, "no_retention/deleted/old.parquet", 4_000, 40);
        // Neither deleted nor under a retention period.
        let kept = parquet_file(7, "no_retention/kept/old.parquet", 8_000, 80);

        let snapshot = snapshot_with(&[
            (retained_db, table_a, &expired_a),
            (retained_db, table_b, &expired_b),
            (other_db, deleted_table, &deleted),
            (other_db, kept_table, &kept),
        ]);
        let persisted_files = PersistedFiles::new_from_persisted_snapshots(vec![snapshot.clone()]);
        persisted_files.delete_table(other_db, deleted_table);

        let removed = persisted_files.remove_files_for_deletion(catalog);

        assert_eq!(removed.len(), 2);
        let retained_removed = &removed.get(&retained_db).unwrap().tables;
        assert_eq!(retained_removed.len(), 2);
        assert_eq!(retained_removed.get(&table_a).unwrap(), &vec![expired_a]);
        assert_eq!(retained_removed.get(&table_b).unwrap(), &vec![expired_b]);
        let other_removed = &removed.get(&other_db).unwrap().tables;
        assert_eq!(other_removed.len(), 1);
        assert_eq!(other_removed.get(&deleted_table).unwrap(), &vec![deleted]);

        assert!(persisted_files.get_files(retained_db, table_a).is_empty());
        assert!(persisted_files.get_files(retained_db, table_b).is_empty());
        assert!(
            persisted_files
                .get_files(other_db, deleted_table)
                .is_empty()
        );
        assert_eq!(persisted_files.get_files(other_db, kept_table), vec![kept]);
        assert_metrics(&persisted_files, 1, 8_000, 80);

        let reloaded = reload_after_removal(snapshot, removed);
        for (db_id, table_id) in [
            (retained_db, table_a),
            (retained_db, table_b),
            (other_db, deleted_table),
            (other_db, kept_table),
        ] {
            assert_eq!(
                reloaded.get_files(db_id, table_id),
                persisted_files.get_files(db_id, table_id)
            );
        }
        assert_metrics(&reloaded, 1, 8_000, 80);
    }

    #[test]
    fn add_persisted_file_replaces_entry_with_same_path() {
        let db_id = DbId::from(0);
        let table_id = TableId::from(0);
        let old = parquet_file(1, "gen2/2026-09-18/00-00/0.parquet", 6_730, 10);
        let other = parquet_file(2, "gen2/2026-09-18/01-00/0.parquet", 1_000, 5);
        let new = parquet_file(3, "gen2/2026-09-18/00-00/0.parquet", 6_750, 12);

        let persisted_files = PersistedFiles::new();
        persisted_files.add_persisted_file(&db_id, &table_id, &old);
        persisted_files.add_persisted_file(&db_id, &table_id, &other);
        assert_metrics(&persisted_files, 2, 6_730 + 1_000, 15);

        persisted_files.add_persisted_file(&db_id, &table_id, &new);
        let mut files = persisted_files.get_files(db_id, table_id);
        files.sort_by_key(|f| f.id);
        assert_eq!(files, vec![other.clone(), new.clone()]);
        assert_metrics(&persisted_files, 2, 6_750 + 1_000, 17);

        // Re-adding the identical entry changes nothing.
        persisted_files.add_persisted_file(&db_id, &table_id, &new);
        assert_eq!(persisted_files.get_files(db_id, table_id).len(), 2);
        assert_metrics(&persisted_files, 2, 6_750 + 1_000, 17);

        // The same path in another table is a different file.
        persisted_files.add_persisted_file(&db_id, &TableId::from(1), &new);
        assert_eq!(persisted_files.get_files(db_id, table_id).len(), 2);
        assert_eq!(persisted_files.get_files(db_id, TableId::from(1)).len(), 1);
        assert_metrics(&persisted_files, 3, 6_750 * 2 + 1_000, 29);
    }

    #[test]
    fn load_snapshots_collapses_same_path_to_newest_entry() {
        let db_id = DbId::from(0);
        let table_id = TableId::from(0);
        let path = "node/dbs/db-0/t-0/2026-09-18/00-00/0000000042.parquet";
        let older = parquet_file(1, path, 6_730, 10);
        let unrelated = parquet_file(2, "node/dbs/db-0/t-0/2026-09-18/00-10/43.parquet", 500, 1);
        let newer = parquet_file(5, path, 6_750, 12);
        let within_snapshot_dupe_a = parquet_file(6, "p/dup.parquet", 100, 1);
        let within_snapshot_dupe_b = parquet_file(7, "p/dup.parquet", 200, 2);

        let snapshot_1 = build_snapshot(vec![older, unrelated.clone()], 1, 1, 1);
        let snapshot_2 = build_snapshot(
            vec![
                newer.clone(),
                within_snapshot_dupe_a,
                within_snapshot_dupe_b.clone(),
            ],
            2,
            2,
            2,
        );

        // `Persister::load_snapshots` returns the newest snapshot first.
        let persisted_files =
            PersistedFiles::new_from_persisted_snapshots(vec![snapshot_2, snapshot_1]);

        let mut files = persisted_files.get_files(db_id, table_id);
        files.sort_by_key(|f| f.id);
        assert_eq!(files, vec![unrelated, newer, within_snapshot_dupe_b]);
        assert_metrics(&persisted_files, 3, 500 + 6_750 + 200, 1 + 12 + 2);
    }

    #[test]
    fn load_snapshots_applies_removals_from_newer_snapshots() {
        let db_id = DbId::from(0);
        let table_id = TableId::from(0);
        let kept = parquet_file(1, "a.parquet", 100, 1);
        let removed = parquet_file(2, "b.parquet", 200, 2);
        let superseded = parquet_file(3, "c.parquet", 300, 3);
        let replacement = parquet_file(4, "c.parquet", 310, 4);

        let snapshot_1 = build_snapshot(
            vec![kept.clone(), removed.clone(), superseded.clone()],
            1,
            1,
            1,
        );
        let snapshot_2 = build_snapshot(vec![replacement.clone()], 2, 2, 2);
        let mut snapshot_3 = build_snapshot(vec![], 3, 3, 3);
        let mut removed_tables = DatabaseTables::default();
        // Removing `superseded` by id must not drop its same-path replacement.
        removed_tables
            .tables
            .insert(table_id, vec![removed, superseded]);
        snapshot_3.removed_files.insert(db_id, removed_tables);

        let persisted_files =
            PersistedFiles::new_from_persisted_snapshots(vec![snapshot_3, snapshot_2, snapshot_1]);

        let mut files = persisted_files.get_files(db_id, table_id);
        files.sort_by_key(|f| f.id);
        assert_eq!(files, vec![kept, replacement]);
        assert_metrics(&persisted_files, 2, 100 + 310, 1 + 4);
    }

    /// The WAL replay on startup starts after the newest snapshot's number, so that is where the
    /// index starts; later snapshots only move it forward.
    #[test]
    fn test_last_persisted_wal_sequence_number_starts_at_newest_and_never_moves_back() {
        assert_eq!(
            PersistedFiles::new().last_persisted_wal_sequence_number(),
            None
        );

        let persisted_files = PersistedFiles::new_from_persisted_snapshots(vec![
            build_snapshot(vec![], 3, 7, 3),
            build_snapshot(vec![], 2, 5, 2),
        ]);
        assert_eq!(
            persisted_files.last_persisted_wal_sequence_number(),
            Some(WalFileSequenceNumber::new(7))
        );

        persisted_files.set_last_persisted_wal_sequence_number(WalFileSequenceNumber::new(6));
        assert_eq!(
            persisted_files.last_persisted_wal_sequence_number(),
            Some(WalFileSequenceNumber::new(7))
        );

        persisted_files.set_last_persisted_wal_sequence_number(WalFileSequenceNumber::new(9));
        assert_eq!(
            persisted_files.last_persisted_wal_sequence_number(),
            Some(WalFileSequenceNumber::new(9))
        );
    }
}
