//! Deleting the objects of parquet files that retention or a hard delete removed from the index
//! (TAG-2753).
//!
//! A flush records such a removal in its snapshot, marked with
//! [`PersistedSnapshot::deletes_removed_objects`], and deletes the objects only once that
//! snapshot is persisted ([`delete_removed_files`]). A stop before those deletes finish leaves
//! objects that no index entry points to. So startup deletes again the objects of the marked
//! removals in the snapshots it loads, and each index checkpoint deletes those of the snapshots
//! it folds before it is written: a checkpoint replaces those snapshots at the next start.
//!
//! Both skip a path that the index still holds. The index can keep a newer entry with the path
//! of a removed one, and the object then belongs to that entry. A path that the loaded index
//! doesn't hold is never written again: gen1 paths carry a WAL number, and replay and new writes
//! use numbers above the loaded snapshot's; compacted paths carry a new UUID.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use influxdb3_id::{DbId, TableId};
use object_store::path::Path;
use object_store::{Error, ObjectStore};
use observability_deps::tracing::warn;

use crate::PersistedSnapshot;
use crate::write_buffer::persisted_files::SnapshotFold;

/// Delete the objects of files that retention or a hard delete removed from the index. Call it
/// only once the snapshot that records their removal is persisted: before that, a restart loads
/// older snapshots that still list them.
///
/// Each file is deleted by its own task, on a best effort basis: a file that isn't deleted is no
/// longer referenced, so it is left in object storage with a warning.
pub(crate) fn delete_removed_files(object_store: &Arc<dyn ObjectStore>, paths: Vec<Path>) {
    for path in paths {
        let object_store = Arc::clone(object_store);
        tokio::spawn(async move {
            for attempt in 1..=REMOVED_FILE_DELETE_ATTEMPTS {
                match object_store.delete(&path).await {
                    // Deleted, or already gone.
                    Ok(()) | Err(Error::NotFound { .. }) => return,
                    Err(e) if attempt == REMOVED_FILE_DELETE_ATTEMPTS => warn!(
                        %path,
                        %e,
                        attempts = attempt,
                        "Failed to delete a parquet file removed by retention or a hard delete; leaving it in object storage"
                    ),
                    Err(_) => tokio::time::sleep(Duration::from_secs(attempt * 10)).await,
                }
            }
        });
    }
}

/// How many times [`delete_removed_files`] tries each delete. The waits between tries grow by
/// 10 s each (10 s, 20 s, ..., 100 s): 550 s, about 9 minutes, in all.
const REMOVED_FILE_DELETE_ATTEMPTS: u64 = 11;

/// The removals of marked snapshots ([`PersistedSnapshot::deletes_removed_objects`]), collected
/// as the snapshots are folded into an index.
#[derive(Debug, Default, Clone)]
pub(crate) struct MarkedRemovals(BTreeSet<(DbId, TableId, String)>);

impl MarkedRemovals {
    /// Collect `snapshot`'s removals if it is marked.
    pub(crate) fn collect(&mut self, snapshot: &PersistedSnapshot) {
        if !snapshot.deletes_removed_objects {
            return;
        }
        for (db_id, tables) in &snapshot.removed_files {
            for (table_id, files) in &tables.tables {
                for file in files {
                    self.0.insert((*db_id, *table_id, file.path.clone()));
                }
            }
        }
    }

    /// The paths of the collected removals that `fold` doesn't index, in path order: no entry
    /// points to their objects, so they can be deleted.
    pub(crate) fn unindexed(&self, fold: &SnapshotFold) -> Vec<Path> {
        let paths: BTreeSet<&str> = self
            .0
            .iter()
            .filter(|(db_id, table_id, path)| !fold.indexes(*db_id, *table_id, path))
            .map(|(_, _, path)| path.as_str())
            .collect();
        paths.into_iter().map(Path::from).collect()
    }

    pub(crate) fn clear(&mut self) {
        self.0.clear();
    }
}

/// Delete each of `paths` once, at most `concurrency` at a time, and wait for them: the index
/// checkpoint writer's pass (see the module docs). Returns how many deletes failed; NotFound
/// counts as deleted.
pub(crate) async fn delete_unindexed_now(
    object_store: &Arc<dyn ObjectStore>,
    paths: Vec<Path>,
    concurrency: usize,
) -> usize {
    futures::stream::iter(paths)
        .map(|path| {
            let object_store = Arc::clone(object_store);
            async move {
                match object_store.delete(&path).await {
                    Ok(()) | Err(Error::NotFound { .. }) => 0,
                    Err(e) => {
                        warn!(%path, %e, "Failed to delete a parquet file removed by retention or a hard delete; leaving it in object storage");
                        1
                    }
                }
            }
        })
        .buffer_unordered(concurrency)
        .fold(0, |failed, f| async move { failed + f })
        .await
}

#[cfg(test)]
mod tests;
