use crate::persister::Persister;
use crate::write_buffer::persisted_files::PersistedFiles;
use crate::{ParquetFile, ParquetFileId, PersistedSnapshot, PersistedSnapshotVersion, WriteBuffer};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion_util::stream_from_batches;
use influxdb3_catalog::catalog::Catalog;
use influxdb3_id::{DbId, TableId};
use iox_query::exec::Executor;
use iox_query::frontend::reorg::ReorgPlanner;
use iox_time::TimeProvider;
use object_store::ObjectStore;
use object_store::path::Path as ObjPath;
use observability_deps::tracing::{debug, error, info, warn};
use schema::Schema;
use schema::sort::SortKey;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;
use uuid::Uuid;

/// Configuration for the compaction service
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Whether compaction is enabled
    pub enabled: bool,
    /// Interval between compaction runs
    pub interval: Duration,
    /// Maximum number of files to compact in a single run
    pub max_files_per_run: usize,
    /// Minimum number of files required before triggering compaction
    pub min_files_for_compaction: usize,
    /// Generation durations for each level
    pub generation_durations: HashMap<u8, Duration>,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval: Duration::from_secs(3600), // 1 hour
            max_files_per_run: 100,
            min_files_for_compaction: 10,
            generation_durations: HashMap::new(),
        }
    }
}

/// Represents a compaction job that needs to be executed
#[derive(Debug, Clone)]
pub struct CompactionJob {
    pub database_id: DbId,
    pub database_name: Arc<str>,
    pub table_id: TableId,
    pub table_name: Arc<str>,
    pub source_generation: u8,
    pub target_generation: u8,
    pub files: Vec<ParquetFile>,
    pub schema: Schema,
    pub sort_key: SortKey,
}

/// Result of a compaction operation
#[derive(Debug)]
pub struct CompactionResult {
    pub compacted_files: Vec<ParquetFile>,
    pub deleted_files: Vec<ParquetFile>,
    pub total_size_reduction: u64,
    pub total_rows_compacted: u64,
}

#[cfg(test)]
mod drift_tests;

#[derive(Debug)]
pub struct CompactionService {
    config: CompactionConfig,
    catalog: Arc<Catalog>,
    write_buffer: Arc<dyn WriteBuffer>,
    persisted_files: Arc<PersistedFiles>,
    persister: Arc<Persister>,
    executor: Arc<Executor>,
    datafusion_config: HashMap<String, String>,
    object_store: Arc<dyn ObjectStore>,
    object_store_url: ObjectStoreUrl,
    node_identifier_prefix: String,
    time_provider: Arc<dyn TimeProvider>,
    shutdown_token: influxdb3_shutdown::ShutdownToken,
}

impl CompactionService {
    pub fn new(
        config: CompactionConfig,
        catalog: Arc<Catalog>,
        write_buffer: Arc<dyn WriteBuffer>,
        persisted_files: Arc<PersistedFiles>,
        persister: Arc<Persister>,
        executor: Arc<Executor>,
        datafusion_config: HashMap<String, String>,
        object_store: Arc<dyn ObjectStore>,
        object_store_url: ObjectStoreUrl,
        node_identifier_prefix: impl Into<String>,
        time_provider: Arc<dyn TimeProvider>,
        shutdown_token: influxdb3_shutdown::ShutdownToken,
    ) -> Self {
        Self {
            config,
            catalog,
            write_buffer,
            persisted_files,
            persister,
            executor,
            datafusion_config,
            object_store,
            object_store_url,
            node_identifier_prefix: node_identifier_prefix.into(),
            time_provider,
            shutdown_token,
        }
    }

    fn persisted_files(&self) -> Arc<PersistedFiles> {
        Arc::clone(&self.persisted_files)
    }

    /// Drop catalog entries whose objects are missing from object storage.
    async fn filter_existing_files(
        &self,
        _db_id: DbId,
        _table_id: TableId,
        files: Vec<ParquetFile>,
    ) -> Result<Vec<ParquetFile>> {
        // Checking every catalog entry against object store (object_store.head per file) is
        // O(n) HTTP requests and prohibitively slow for large catalogs (e.g. 477k files → minutes
        // of sequential HEAD calls before a single job can be identified). Trust the catalog
        // during planning. Missing objects and size mismatches (stale entries for overwritten
        // objects) are detected and pruned per job in execute_compaction_job, which only HEADs
        // the files of that job.
        Ok(files)
    }

    /// Start the background compaction service
    pub fn start(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            if !self.config.enabled {
                info!("Compaction service is disabled");
                return;
            }

            info!("Starting compaction service with interval: {:?}", self.config.interval);
            
            let mut interval = tokio::time::interval(self.config.interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(e) = Arc::clone(&self).run_compaction_cycle().await {
                            error!("Compaction cycle failed: {}", e);
                        }
                    }
                    _ = self.shutdown_token.wait_for_shutdown() => {
                        info!("Shutdown signal received, stopping compaction service");
                        break;
                    }
                }
            }
        })
    }

    /// Run a single compaction cycle
    async fn run_compaction_cycle(self: &Arc<Self>) -> Result<()> {
        debug!("Starting compaction cycle");
        
        let jobs = self.identify_compaction_jobs().await?;
        if jobs.is_empty() {
            debug!("No compaction jobs identified");
            return Ok(());
        }

        info!("Identified {} compaction jobs", jobs.len());
        
        let mut set = JoinSet::new();
        let mut completed_jobs = 0;
        let max_concurrent = std::cmp::min(jobs.len(), 4); // Limit concurrent compactions

        for job in jobs.into_iter().take(self.config.max_files_per_run) {
            if set.len() >= max_concurrent {
                if let Some(result) = set.join_next().await {
                    match result {
                        Ok(Ok(_)) => completed_jobs += 1,
                        Ok(Err(e)) => error!("Compaction job failed: {:#}", e),
                        Err(e) => error!("Compaction task failed: {}", e),
                    }
                }
            }

            let service = Arc::clone(self);
            set.spawn(async move {
                service.execute_compaction_job(job).await
            });
        }

        // Wait for remaining jobs
        while let Some(result) = set.join_next().await {
            match result {
                Ok(Ok(_)) => completed_jobs += 1,
                Ok(Err(e)) => error!("Compaction job failed: {:#}", e),
                Err(e) => error!("Compaction task failed: {}", e),
            }
        }

        info!("Compaction cycle completed: {} jobs processed", completed_jobs);
        Ok(())
    }

    /// Identify files that need compaction
    async fn identify_compaction_jobs(&self) -> Result<Vec<CompactionJob>> {
        let mut jobs = Vec::new();
        let now = self.time_provider.now().timestamp_nanos();

        // Get all databases and tables
        let databases = self.catalog.list_db_schema();
        
        for db_schema in databases {
            if db_schema.deleted {
                continue;
            }

            for table_def in db_schema.tables() {
                if table_def.deleted {
                    continue;
                }

                // Get files for this table, skipping catalog entries with no object store object
                let files = self
                    .filter_existing_files(
                        db_schema.id,
                        table_def.table_id,
                        self.write_buffer
                            .parquet_files(db_schema.id, table_def.table_id),
                    )
                    .await?;
                if files.len() < self.config.min_files_for_compaction {
                    continue;
                }

                // Group files by generation level and check for compaction opportunities
                let mut files_by_generation: BTreeMap<u8, Vec<ParquetFile>> = BTreeMap::new();
                
                for file in files {
                    let generation = self.get_file_generation(&file)?;
                    files_by_generation.entry(generation).or_default().push(file);
                }

                // Check each generation level for compaction opportunities
                for (current_gen, files) in files_by_generation.iter() {
                    if files.len() < self.config.min_files_for_compaction {
                        continue;
                    }

                    // Check if we can compact to the next generation
                    if let Some(next_gen) = self.get_next_generation(*current_gen) {
                        if let Some(target_duration) = self.config.generation_durations.get(&next_gen) {
                            for batch in group_files_by_time_window(
                                files,
                                *target_duration,
                                self.config.min_files_for_compaction,
                                self.config.max_files_per_run,
                            ) {
                                if window_has_ended(&batch, *target_duration, now) {
                                    jobs.push(CompactionJob {
                                        database_id: db_schema.id,
                                        database_name: Arc::clone(&db_schema.name),
                                        table_id: table_def.table_id,
                                        table_name: Arc::clone(&table_def.table_name),
                                        source_generation: *current_gen,
                                        target_generation: next_gen,
                                        files: batch,
                                        schema: table_def.schema.clone(),
                                        sort_key: table_def.sort_key.clone(),
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(jobs)
    }

    /// Execute a single compaction job
    async fn execute_compaction_job(&self, job: CompactionJob) -> Result<CompactionResult> {
        info!(
            "Starting compaction job: db={}, table={}, gen{}->gen{}",
            job.database_id, job.table_name, job.source_generation, job.target_generation
        );

        // Validate sort key configuration
        if job.sort_key.is_empty() {
            return Err(anyhow::anyhow!("Cannot compact table {}: sort key is empty", job.table_name));
        }

        let start_time = std::time::Instant::now();

        // Filter out catalog entries that do not describe what is in object storage (catalog
        // drift): the object is missing, or its size differs from the indexed size (the object was
        // overwritten after the entry was recorded; reading it with the stale size fails with
        // "Invalid Parquet file. Corrupt footer"). Done per-job (typically 100s of files) rather
        // than pre-planning (potentially 100k+ files) so this remains fast even with large catalogs.
        let files = {
            let mut existing = Vec::with_capacity(job.files.len());
            let mut stale = Vec::new();
            for file in &job.files {
                let path = ObjPath::from(file.path.as_str());
                match self.object_store.head(&path).await {
                    Ok(meta) if meta.size as u64 == file.size_bytes => existing.push(file.clone()),
                    Ok(meta) => {
                        // Pruning drops the entry, so the object's rows stop being queryable.
                        // The object itself is left in place; the path logged here is enough
                        // to re-index or inspect it manually.
                        warn!(
                            "Skipping parquet file whose size does not match the catalog: {} (catalog {} bytes, object store {} bytes)",
                            file.path, file.size_bytes, meta.size
                        );
                        stale.push(file.clone());
                    }
                    Err(object_store::Error::NotFound { .. }) => {
                        warn!("Skipping missing parquet file in catalog: {}", file.path);
                        stale.push(file.clone());
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            if !stale.is_empty() {
                // Removal is by file id, so pruning a stale entry never drops a valid entry that
                // shares its path.
                self.persisted_files()
                    .remove_persisted_files(&job.database_id, &job.table_id, &stale);
                info!(
                    "Pruned {} stale catalog entries for db={} table={}",
                    stale.len(), job.database_id, job.table_name
                );
            }
            if existing.len() < self.config.min_files_for_compaction {
                info!(
                    "Skipping compaction for db={} table={}: only {} files remain after drift check (min {})",
                    job.database_id, job.table_name, existing.len(), self.config.min_files_for_compaction
                );
                return Ok(CompactionResult {
                    compacted_files: vec![],
                    deleted_files: vec![],
                    total_size_reduction: 0,
                    total_rows_compacted: 0,
                });
            }
            existing
        };

        let total_input_size: u64 = files.iter().map(|f| f.size_bytes).sum();
        let _total_input_rows: u64 = files.iter().map(|f| f.row_count).sum();

        // Create chunks from the parquet files
        let chunks = self.create_chunks_from_files(&files, &job.schema).await?;

        // Execute compaction using DataFusion (honor server datafusion config, e.g. max_parquet_fanout)
        let ctx = {
            let mut session_config = self.executor.new_session_config();
            for (key, value) in &self.datafusion_config {
                session_config = session_config.with_config_option(key, value);
            }
            session_config.build()
        };
        
        info!(
            "Creating compaction plan with sort key: {:?} for table {}",
            job.sort_key, job.table_name
        );
        
        let logical_plan = ReorgPlanner::new()
            .compact_plan(
                data_types::TableId::new(0),
                job.table_name.clone(),
                &job.schema,
                chunks,
                job.sort_key.clone(),
            )
            .context("failed to create compaction plan")?;

        let physical_plan = ctx
            .create_physical_plan(&logical_plan)
            .await
            .context("failed to create physical plan")?;

        let raw_data = ctx
            .collect(physical_plan)
            .await
            .context("failed to execute compaction")?;

        // gen1 WAL files are stored in ingestion order, not sorted by series key or time.
        // SortPreservingMergeExec assumes sorted inputs; explicitly sort the collected output
        // so gen2 files are correctly ordered for pruning and time-range min/max metadata.
        let data = sort_record_batches(raw_data, &job.sort_key)
            .context("failed to sort compaction output")?;

        // Write compacted data to new files. On failure this removes whatever it uploaded.
        let compacted_files = self.write_compacted_files(
            &job,
            data,
            &job.schema,
        ).await?;

        // Validate that the compacted data is properly sorted. The outputs are not referenced by
        // the index yet, so on failure delete them rather than leaving orphans behind.
        // validate_compacted_data currently always succeeds; this keeps cleanup correct if it
        // starts rejecting output.
        if let Err(e) = self.validate_compacted_data(&compacted_files).await {
            let paths: Vec<ObjPath> = compacted_files
                .iter()
                .map(|f| ObjPath::from(f.path.as_str()))
                .collect();
            self.delete_unreferenced_outputs(&paths).await;
            return Err(e);
        }

        // Update catalog: add new compacted files and remove old files. No cleanup on failure
        // here: the new files are added to the index first, so deleting them could leave the
        // index pointing at missing objects.
        self.update_catalog_for_compaction(&job, &compacted_files, &files).await?;

        // Calculate results
        let total_output_size: u64 = compacted_files.iter().map(|f| f.size_bytes).sum();
        let total_output_rows: u64 = compacted_files.iter().map(|f| f.row_count).sum();
        let size_reduction = total_input_size.saturating_sub(total_output_size);

        let result = CompactionResult {
            compacted_files,
            deleted_files: files.clone(),
            total_size_reduction: size_reduction,
            total_rows_compacted: total_output_rows,
        };

        let duration = start_time.elapsed();
        let files_len = result.deleted_files.len();
        info!(
            "Compaction completed: {} files -> {} files, {} rows, {} bytes -> {} bytes ({}% reduction) in {:?}",
            files_len,
            result.compacted_files.len(),
            total_output_rows,
            total_input_size,
            total_output_size,
            if total_input_size > 0 { (size_reduction * 100) / total_input_size } else { 0 },
            duration
        );

        // Log detailed compaction statistics
        self.log_compaction_statistics(&job, &result, duration).await;

        Ok(result)
    }

    /// Create DataFusion chunks from parquet files
    async fn create_chunks_from_files(
        &self,
        files: &[ParquetFile],
        schema: &Schema,
    ) -> Result<Vec<Arc<dyn iox_query::QueryChunk>>> {
        let mut chunks = Vec::new();
        
        for (i, file) in files.iter().enumerate() {
            let chunk = crate::write_buffer::parquet_chunk_from_file(
                file,
                schema,
                self.object_store_url.clone(),
                Arc::clone(&self.object_store),
                i as i64,
            );
            chunks.push(Arc::new(chunk) as Arc<dyn iox_query::QueryChunk>);
        }

        Ok(chunks)
    }

    /// Write compacted data to new parquet files.
    ///
    /// Every output gets a fresh unique name, so a retry never overwrites (reclaims) what a failed
    /// attempt uploaded. If any step fails, the objects uploaded by this call are therefore
    /// deleted (best effort) before the original error is returned.
    async fn write_compacted_files(
        &self,
        job: &CompactionJob,
        data: Vec<arrow::record_batch::RecordBatch>,
        schema: &Schema,
    ) -> Result<Vec<ParquetFile>> {
        let mut uploaded = Vec::new();
        match self
            .try_write_compacted_files(job, data, schema, &mut uploaded)
            .await
        {
            Ok(files) => Ok(files),
            Err(e) => {
                self.delete_unreferenced_outputs(&uploaded).await;
                Err(e)
            }
        }
    }

    /// Body of [`Self::write_compacted_files`]. Each path is pushed to `uploaded` before its `put`
    /// is issued, so an upload whose outcome is unknown (e.g. timed out) is cleaned up too.
    async fn try_write_compacted_files(
        &self,
        job: &CompactionJob,
        data: Vec<arrow::record_batch::RecordBatch>,
        schema: &Schema,
        uploaded: &mut Vec<ObjPath>,
    ) -> Result<Vec<ParquetFile>> {
        let mut compacted_files = Vec::new();

        for batch in data {
            if batch.num_rows() == 0 {
                continue;
            }

            // Calculate min_time and max_time from the batch
            let (min_time, max_time) = self.calculate_time_range_from_batch(&batch)?;
            
            // Generate new file path for the target generation
            let target_duration = self.config.generation_durations.get(&job.target_generation)
                .ok_or_else(|| anyhow::anyhow!("No duration configured for generation {}", job.target_generation))?;
            
            let chunk_time = self.calculate_chunk_time_for_generation(&batch, target_duration);
            let path = self.generate_file_path(job, job.target_generation, chunk_time);

            // Write the batch to parquet
            let batch_stream = stream_from_batches(schema.as_arrow(), vec![batch.clone()]);
            let parquet_bytes = crate::persister::serialize_to_parquet(
                Arc::new(datafusion::execution::memory_pool::UnboundedMemoryPool::default()),
                batch_stream,
            ).await?;

            uploaded.push(path.clone());
            self.object_store
                .put(&path, parquet_bytes.bytes.clone().into())
                .await
                .with_context(|| format!("failed to upload compacted parquet to {}", path))?;

            let parquet_file = ParquetFile {
                id: ParquetFileId::new(),
                path: path.to_string(),
                size_bytes: parquet_bytes.bytes.len() as u64,
                row_count: batch.num_rows() as u64,
                chunk_time,
                min_time,
                max_time,
            };

            compacted_files.push(parquet_file);
        }

        Ok(compacted_files)
    }

    /// Best-effort delete of compaction outputs that the index does not reference, after a
    /// failed attempt. Failures are logged and otherwise ignored so the caller can return the
    /// original error.
    async fn delete_unreferenced_outputs(&self, paths: &[ObjPath]) {
        for path in paths {
            match self.object_store.delete(path).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => warn!(
                    "Failed to delete orphaned compaction output {}: {}",
                    path, e
                ),
            }
        }
    }

    /// Validate that the compacted data was written successfully.
    ///
    /// Note: after sorting by the full series key (company, site, asset_id, ..., time),
    /// time is monotonic per-series but NOT globally across files — different asset_ids
    /// interleave in time. We only verify file count here; min/max metadata is computed
    /// correctly per-batch by calculate_time_range_from_batch.
    async fn validate_compacted_data(&self, compacted_files: &[ParquetFile]) -> Result<()> {
        info!("Compacted into {} output file(s)", compacted_files.len());
        Ok(())
    }

    /// Calculate min_time and max_time from a record batch
    fn calculate_time_range_from_batch(&self, batch: &arrow::record_batch::RecordBatch) -> Result<(i64, i64)> {
        // Find the time column index
        let time_col_idx = batch
            .schema()
            .fields()
            .iter()
            .position(|field| field.name() == "time")
            .ok_or_else(|| anyhow::anyhow!("No time column found in batch"))?;

        // Get the time column as a timestamp array
        let time_array = batch
            .column(time_col_idx)
            .as_any()
            .downcast_ref::<arrow::array::TimestampNanosecondArray>()
            .ok_or_else(|| anyhow::anyhow!("Time column is not a timestamp array"))?;

        if time_array.len() == 0 {
            return Ok((0, 0));
        }

        // After sorting by the full series key (asset_id, meter, ..., time), time is monotonic
        // per-series but NOT globally — rows from ASSET002 (older timestamps) follow ASSET001
        // (newer timestamps). Compute actual min/max across the whole batch instead of
        // assuming global monotonicity.
        let min_time = arrow::compute::min(time_array)
            .ok_or_else(|| anyhow::anyhow!("Failed to compute min time"))?;
        let max_time = arrow::compute::max(time_array)
            .ok_or_else(|| anyhow::anyhow!("Failed to compute max time"))?;

        Ok((min_time, max_time))
    }

    /// Get the generation level for a file based on its path
    fn get_file_generation(&self, file: &ParquetFile) -> Result<u8> {
        // Parse generation from file path
        // Expected format: dbs/{table}-{db_id}/{table}-{table_id}/gen{level}/{YYYY-MM-DD}/{HH-MM}/{uuid}.parquet
        let path = &file.path;
        
        // Look for "gen{level}" in the path
        if let Some(gen_start) = path.find("/gen") {
            let gen_part = &path[gen_start + 4..]; // Skip "/gen"
            if let Some(gen_end) = gen_part.find('/') {
                let gen_str = &gen_part[..gen_end];
                match gen_str.parse::<u8>() {
                    Ok(generation) if generation >= 1 && generation <= 5 => Ok(generation),
                    Ok(generation) => Err(anyhow::anyhow!("Invalid generation number {} in path: {}", generation, path)),
                    Err(e) => Err(anyhow::anyhow!("Invalid generation in path {}: {}", path, e)),
                }
            } else {
                Err(anyhow::anyhow!("Could not find generation end marker in path: {}", path))
            }
        } else {
            // If no generation found in path, assume gen1
            debug!("No generation found in path {}, assuming gen1", path);
            Ok(1)
        }
    }

    /// Get the next generation level
    fn get_next_generation(&self, current_gen: u8) -> Option<u8> {
        if current_gen < 5 {
            Some(current_gen + 1)
        } else {
            None
        }
    }

    /// Calculate chunk time for a generation based on the batch data and target duration
    fn calculate_chunk_time_for_generation(
        &self,
        batch: &arrow::record_batch::RecordBatch,
        target_duration: &Duration,
    ) -> i64 {
        // Calculate min_time from the batch
        if let Ok((min_time, _)) = self.calculate_time_range_from_batch(batch) {
            if min_time == 0 {
                // Fallback to current time if min_time is 0
                return self.time_provider.now().timestamp_nanos();
            }
            
            // Round down to the nearest target duration boundary
            let duration_nanos = target_duration.as_nanos() as i64;
            if duration_nanos > 0 {
                (min_time / duration_nanos) * duration_nanos
            } else {
                min_time
            }
        } else {
            // Fallback to current time if we can't calculate from batch
            warn!("Could not calculate time range from batch, using current time");
            self.time_provider.now().timestamp_nanos()
        }
    }

    /// Generate file path for a generation
    fn generate_file_path(
        &self,
        job: &CompactionJob,
        generation: u8,
        chunk_time: i64,
    ) -> ObjPath {
        compacted_file_path(
            &self.node_identifier_prefix,
            job,
            generation,
            chunk_time,
            Uuid::now_v7(),
        )
    }

    /// Update catalog for compaction: persist snapshot, then update catalog and remove old files.
    async fn update_catalog_for_compaction(
        &self,
        job: &CompactionJob,
        new_files: &[ParquetFile],
        old_files: &[ParquetFile],
    ) -> Result<()> {
        let wal = self.write_buffer.wal();
        let snapshot_sequence_number = wal.reserve_next_snapshot_sequence_number().await;
        // A data snapshot with a lower sequence number may still be writing its files; it holds
        // the WAL's snapshot permit until it is done. Wait for it, so that this snapshot records
        // its WAL number and a `next_file_id` above its file ids. The number is reserved first
        // because a flush that starts a snapshot waits for this permit while holding the WAL lock.
        let snapshot_permit = wal.acquire_snapshot_permit().await;
        // This snapshot becomes the newest, and startup replays the WAL after its
        // `wal_file_sequence_number`. It persists no WAL data, so it carries the number of the last
        // snapshot that did: the last WAL file written would skip rows that exist only in the WAL.
        let wal_file_sequence_number = self
            .persisted_files()
            .last_persisted_wal_sequence_number()
            .unwrap_or_default();
        let mut snapshot = PersistedSnapshot::new(
            self.node_identifier_prefix.clone(),
            snapshot_sequence_number,
            wal_file_sequence_number,
            self.catalog.sequence_number(),
        );
        for file in new_files {
            snapshot.add_parquet_file(job.database_id, job.table_id, file.clone());
        }
        snapshot
            .removed_files
            .entry(job.database_id)
            .or_default()
            .tables
            .entry(job.table_id)
            .or_default()
            .extend(old_files.iter().cloned());
        // Release the permit before the upload, which retries until it succeeds: a flush that
        // starts a data snapshot waits for it while holding the WAL lock, and so does every write.
        // A data snapshot that starts now gets a higher number than this one, so it is newer
        // whichever of the two is written first.
        drop(snapshot_permit);

        // Persist snapshot before mutating catalog or deleting old parquet objects.
        let persisted_snapshot = PersistedSnapshotVersion::V1(snapshot);
        loop {
            match self.persister.persist_snapshot(&persisted_snapshot).await {
                Ok(_) => break,
                Err(e) => {
                    error!(%e, "Error persisting compaction snapshot, sleeping and retrying...");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }

        let persisted_files = self.persisted_files();
        for file in new_files {
            persisted_files.add_persisted_file(&job.database_id, &job.table_id, file);
        }
        persisted_files.remove_persisted_files(&job.database_id, &job.table_id, old_files);

        // Objects are deleted by path while the index removed entries by id. This assumes no
        // other index entry shares an input's path, which holds because gen1 paths carry the WAL
        // sequence number and compacted paths carry a UUID.
        for file in old_files {
            let path = ObjPath::from(file.path.clone());
            if let Err(e) = self.object_store.delete(&path).await {
                warn!("Failed to delete old gen{} file {}: {}", job.source_generation, file.path, e);
            }
        }

        Ok(())
    }

    /// Log detailed compaction statistics
    async fn log_compaction_statistics(
        &self,
        job: &CompactionJob,
        result: &CompactionResult,
        duration: Duration,
    ) {
        let files_len = result.deleted_files.len();
        let compacted_files_len = result.compacted_files.len();
        let total_input_size: u64 = result.deleted_files.iter().map(|f| f.size_bytes).sum();
        let total_output_size: u64 = result.compacted_files.iter().map(|f| f.size_bytes).sum();
        let total_rows_compacted = result.total_rows_compacted;
        let size_reduction = result.total_size_reduction;

        let duration_secs = duration.as_secs();

        info!(
            "Compaction Summary: db={}, table={}, gen{}->gen{}, {} files -> {} files, {} rows, {} bytes -> {} bytes ({}% reduction) in {}s",
            job.database_id,
            job.table_name,
            job.source_generation,
            job.target_generation,
            files_len,
            compacted_files_len,
            total_rows_compacted,
            total_input_size,
            total_output_size,
            if total_input_size > 0 { (size_reduction * 100) / total_input_size } else { 0 },
            duration_secs
        );
    }
}

/// Group source files into time-aligned windows and cap batch size per compaction job.
fn group_files_by_time_window(
    files: &[ParquetFile],
    window: Duration,
    min_files: usize,
    max_files: usize,
) -> Vec<Vec<ParquetFile>> {
    let window_nanos = window.as_nanos() as i64;
    if window_nanos <= 0 || files.len() < min_files {
        return Vec::new();
    }

    let mut by_window: BTreeMap<i64, Vec<ParquetFile>> = BTreeMap::new();
    for file in files {
        let window_start = (file.min_time / window_nanos) * window_nanos;
        by_window
            .entry(window_start)
            .or_default()
            .push(file.clone());
    }

    let mut batches = Vec::new();
    for mut group in by_window.into_values() {
        if group.len() < min_files {
            continue;
        }
        while group.len() >= min_files {
            let take = group.len().min(max_files);
            let batch: Vec<_> = group.drain(..take).collect();
            if batch.len() >= min_files {
                batches.push(batch);
            }
            if group.len() < min_files {
                break;
            }
        }
    }
    batches
}

/// Whether the window that `batch` was grouped into (see [`group_files_by_time_window`]) has
/// ended at `now`. A window is compacted once it has ended, not once its files span it: files
/// are grouped by `min_time`, so a window of shorter source files (e.g. a day of 10 minute gen1
/// files) never spans the whole window.
fn window_has_ended(batch: &[ParquetFile], window: Duration, now: i64) -> bool {
    let window_nanos = window.as_nanos() as i64;
    let Some(min_time) = batch.iter().map(|f| f.min_time).min() else {
        return false;
    };
    if window_nanos <= 0 {
        return false;
    }
    let window_start = (min_time / window_nanos) * window_nanos;
    window_start.saturating_add(window_nanos) <= now
}

/// Sort a collection of record batches by the given sort key.
///
/// gen1 Parquet files are written from WAL flush windows in ingestion order, not sorted
/// by series key or time. This function concatenates all batches and re-sorts them so
/// the compacted gen2 output is correctly ordered for file pruning and time metadata.
fn sort_record_batches(
    batches: Vec<arrow::record_batch::RecordBatch>,
    sort_key: &SortKey,
) -> Result<Vec<arrow::record_batch::RecordBatch>> {
    if batches.is_empty() {
        return Ok(batches);
    }

    let schema = batches[0].schema();
    let combined = arrow::compute::concat_batches(&schema, &batches)
        .context("failed to concatenate batches for sort")?;

    if combined.num_rows() == 0 {
        return Ok(vec![combined]);
    }

    // Build sort columns from the sort key; skip any columns not present in the schema.
    let sort_columns: Vec<arrow::compute::SortColumn> = sort_key
        .iter()
        .filter_map(|(col_name, opts)| {
            schema.index_of(col_name).ok().map(|idx| arrow::compute::SortColumn {
                values: combined.column(idx).clone(),
                options: Some(arrow::compute::SortOptions {
                    descending: opts.descending,
                    nulls_first: opts.nulls_first,
                }),
            })
        })
        .collect();

    if sort_columns.is_empty() {
        return Ok(vec![combined]);
    }

    let indices = arrow::compute::lexsort_to_indices(&sort_columns, None)
        .context("failed to compute sort indices")?;

    let sorted_arrays: Vec<arrow::array::ArrayRef> = combined
        .columns()
        .iter()
        .map(|arr| {
            arrow::compute::take(arr.as_ref(), &indices, None)
                .context("failed to reorder column during sort")
        })
        .collect::<Result<Vec<_>>>()?;

    let sorted = arrow::record_batch::RecordBatch::try_new(schema, sorted_arrays)
        .context("failed to construct sorted record batch")?;

    Ok(vec![sorted])
}

/// Build the object store path for a compacted file.
///
/// The file name is a unique id so that compacting the same time window more than once (e.g. when
/// late gen1 files arrive for a window that already has a gen2 file) never overwrites an existing
/// file that the catalog still references with its old size. Callers pass a UUID v7: being
/// time-ordered, files in a window directory list in the order they were written.
fn compacted_file_path(
    node_identifier_prefix: &str,
    job: &CompactionJob,
    generation: u8,
    chunk_time: i64,
    unique_id: Uuid,
) -> ObjPath {
    let date_time = DateTime::<Utc>::from_timestamp_nanos(chunk_time);
    ObjPath::from(format!(
        "{}/dbs/{}-{}/{}-{}/gen{}/{}/{}.parquet",
        node_identifier_prefix,
        job.database_name,
        job.database_id,
        job.table_name,
        job.table_id,
        generation,
        date_time.format("%Y-%m-%d/%H-%M"),
        unique_id
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Bufferer;

    #[test]
    fn test_compaction_config_default() {
        let config = CompactionConfig::default();
        assert!(config.enabled);
        assert_eq!(config.interval, Duration::from_secs(3600));
        assert_eq!(config.max_files_per_run, 100);
        assert_eq!(config.min_files_for_compaction, 10);
    }

    #[test]
    fn test_get_next_generation() {
        let config = CompactionConfig::default();
        // Note: This test is simplified since we can't easily create mock dependencies
        // In a real implementation, we would use proper mocking
        assert_eq!(config.max_files_per_run, 100);
        assert_eq!(config.min_files_for_compaction, 10);
    }

    #[test]
    fn test_compacted_file_path_format() {
        let job = CompactionJob {
            database_id: DbId::from(2),
            database_name: Arc::from("testdb"),
            table_id: TableId::from(3),
            table_name: Arc::from("alarm_event"),
            source_generation: 1,
            target_generation: 2,
            files: vec![],
            schema: schema::SchemaBuilder::new().timestamp().build().unwrap(),
            sort_key: SortKey::from_columns(vec!["time"]),
        };
        // 2026-09-18T00:00:00Z
        let chunk_time = 1_789_689_600_000_000_000;
        let id = Uuid::now_v7();
        let path = compacted_file_path("node", &job, 2, chunk_time, id).to_string();

        // The database segment (`dbs/{db}-{db_id}`) is deliberately not asserted here.
        assert!(path.starts_with("node/dbs/"), "{path}");
        let (_, rest) = path
            .split_once("/alarm_event-3/gen2/2026-09-18/00-00/")
            .unwrap_or_else(|| panic!("unexpected layout: {path}"));
        let stem = rest
            .strip_suffix(".parquet")
            .unwrap_or_else(|| panic!("missing .parquet suffix: {path}"));
        assert_eq!(Uuid::parse_str(stem).unwrap(), id);
    }

    use crate::persister::{DEFAULT_OBJECT_STORE_URL, Persister};
    use crate::write_buffer::{WriteBufferImpl, WriteBufferImplArgs};
    use influxdb3_cache::distinct_cache::DistinctCacheProvider;
    use influxdb3_cache::last_cache::LastCacheProvider;
    use influxdb3_shutdown::ShutdownManager;
    use influxdb3_wal::WalConfig;
    use iox_time::MockProvider;
    use metric::Registry;
    use object_store::memory::InMemory;

    struct CompactionHarness {
        object_store: Arc<dyn ObjectStore>,
        catalog: Arc<Catalog>,
        persister: Arc<Persister>,
        time: Arc<MockProvider>,
        executor: Arc<Executor>,
        write_buffer: Arc<WriteBufferImpl>,
        service: CompactionService,
    }

    impl CompactionHarness {
        const DB: &'static str = "testdb";
        const TABLE: &'static str = "testtable";

        /// In-memory object store + real `WriteBufferImpl` + `CompactionService` that compacts
        /// gen1 -> gen2 into 2 minute windows once a window has ended and holds a gen1 file.
        /// The clock starts at 0; move it with [`Self::set_now`].
        async fn new() -> Self {
            Self::with_object_store(Arc::new(InMemory::new())).await
        }

        /// Like [`Self::new`], on the given object store.
        async fn with_object_store(object_store: Arc<dyn ObjectStore>) -> Self {
            let config = CompactionConfig {
                enabled: true,
                interval: Duration::from_secs(1),
                max_files_per_run: 10,
                min_files_for_compaction: 1,
                generation_durations: HashMap::from([
                    (1, Duration::from_secs(60)),
                    (2, Duration::from_secs(120)),
                ]),
            };
            Self::build(object_store, config).await
        }

        /// Like [`Self::new`], with the given compaction config.
        async fn with_config(config: CompactionConfig) -> Self {
            Self::build(Arc::new(InMemory::new()), config).await
        }

        async fn build(object_store: Arc<dyn ObjectStore>, config: CompactionConfig) -> Self {
            // Not `Executor::new_testing()`: that shares a process-wide DedicatedExecutor whose IO
            // runtime is the tokio runtime of whichever test created it first, so running a
            // DataFusion plan after that test ends panics with "IO runtime was shut down". A
            // per-test executor captures this test's runtime for IO instead.
            let mut runtime_builder = tokio::runtime::Builder::new_current_thread();
            runtime_builder.enable_time();
            let executor = Arc::new(Executor::new_with_config_and_executor(
                iox_query::exec::ExecutorConfig::testing(),
                executor::DedicatedExecutor::new(
                    "compaction-test",
                    runtime_builder,
                    Arc::new(Registry::default()),
                ),
            ));

            let time = Arc::new(MockProvider::new(iox_time::Time::from_timestamp_nanos(0)));
            let time_provider: Arc<dyn TimeProvider> = Arc::clone(&time) as _;
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
            catalog.create_database(Self::DB).await.unwrap();

            let persister = Arc::new(Persister::new(
                Arc::clone(&object_store),
                "test-host",
                Arc::clone(&time_provider),
            ));
            let write_buffer =
                Self::start_write_buffer(&persister, &catalog, &time_provider, &executor).await;

            let service = CompactionService::new(
                config,
                Arc::clone(&catalog),
                Arc::clone(&write_buffer) as Arc<dyn WriteBuffer>,
                write_buffer.persisted_files(),
                Arc::clone(&persister),
                Arc::clone(&executor),
                // As the server sets it (influxdb3_clap_blocks datafusion config): without this,
                // iox_query encodes a size hint that plain object stores reject.
                HashMap::from([(
                    "iox.hint_known_object_size_to_object_store".to_string(),
                    false.to_string(),
                )]),
                Arc::clone(&object_store),
                ObjectStoreUrl::parse(DEFAULT_OBJECT_STORE_URL).unwrap(),
                "test-host",
                time_provider,
                ShutdownManager::new_testing().register(),
            );

            Self {
                object_store,
                catalog,
                persister,
                time,
                executor,
                write_buffer,
                service,
            }
        }

        /// Build a write buffer the way the server does on startup: load the newest snapshot,
        /// then replay the WAL files after its `wal_file_sequence_number`.
        async fn start_write_buffer(
            persister: &Arc<Persister>,
            catalog: &Arc<Catalog>,
            time_provider: &Arc<dyn TimeProvider>,
            executor: &Arc<Executor>,
        ) -> Arc<WriteBufferImpl> {
            let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(catalog))
                .await
                .unwrap();
            let distinct_cache = DistinctCacheProvider::new_from_catalog(
                Arc::clone(time_provider),
                Arc::clone(catalog),
            )
            .await
            .unwrap();
            WriteBufferImpl::new(WriteBufferImplArgs {
                persister: Arc::clone(persister),
                catalog: Arc::clone(catalog),
                last_cache,
                distinct_cache,
                time_provider: Arc::clone(time_provider),
                executor: Arc::clone(executor),
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
            .unwrap()
        }

        /// Start a second server on the same object store, as a pod restart does. Returns its
        /// catalog and write buffer once the WAL replay has finished.
        ///
        /// The original write buffer keeps running, so don't write to it afterwards: both WALs
        /// would try to create the same WAL file. Starting a write buffer also resets the
        /// process-wide parquet file id counter to the newest snapshot's `next_file_id`.
        async fn restart(&self) -> (Arc<Catalog>, Arc<WriteBufferImpl>) {
            let time_provider: Arc<dyn TimeProvider> = Arc::clone(&self.time) as _;
            let catalog = Arc::new(
                Catalog::new(
                    "test-host",
                    Arc::clone(&self.object_store),
                    Arc::clone(&time_provider),
                    Default::default(),
                )
                .await
                .unwrap(),
            );
            let write_buffer =
                Self::start_write_buffer(&self.persister, &catalog, &time_provider, &self.executor)
                    .await;
            (catalog, write_buffer)
        }

        fn set_now(&self, since_epoch: Duration) {
            let nanos = since_epoch.as_nanos() as i64;
            self.time.set(iox_time::Time::from_timestamp_nanos(nanos));
        }

        /// The newest persisted snapshot: startup replays the WAL after its
        /// `wal_file_sequence_number`.
        async fn newest_snapshot(&self) -> PersistedSnapshot {
            let snapshots = self.persister.load_snapshots(1).await.unwrap();
            match snapshots.into_iter().next().expect("no snapshot persisted") {
                PersistedSnapshotVersion::V1(snapshot) => snapshot,
            }
        }

        /// All files in the index for the test table (empty until the table exists).
        fn indexed_files(&self) -> Vec<ParquetFile> {
            let db = self.catalog.db_schema(Self::DB).unwrap();
            let Some(table) = db.table_definition(Self::TABLE) else {
                return vec![];
            };
            self.write_buffer
                .persisted_files()
                .get_files(db.id, table.table_id)
        }

        fn gen1_file_count(&self) -> usize {
            self.indexed_files()
                .iter()
                .filter(|f| !f.path.contains("/gen"))
                .count()
        }

        /// Write line protocol and force it through a snapshot into a new gen1 parquet file.
        async fn write_gen1(&self, lp: &str) {
            use data_types::NamespaceName;

            let gen1_before = self.gen1_file_count();
            self.write_buffer
                .write_lp(
                    NamespaceName::new(Self::DB).unwrap(),
                    lp,
                    iox_time::Time::from_timestamp_nanos(0),
                    false,
                    crate::Precision::Nanosecond,
                    false,
                )
                .await
                .unwrap();
            if let Some((snapshot_done, _, _permit)) =
                self.write_buffer.wal().force_flush_buffer().await
            {
                snapshot_done.await.expect("snapshot failed");
            }
            for _ in 0..100 {
                if self.gen1_file_count() > gen1_before {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            panic!("no new gen1 file was persisted");
        }

        /// Index `count` gen1 files of 10 minutes each from `start`, one per gen1 chunk, without
        /// writing their objects: planning only reads the index. The table must exist. Returns
        /// their paths.
        fn index_ten_minute_gen1_files(&self, start: i64, count: i64) -> Vec<String> {
            const TEN_MINUTES: i64 = 600_000_000_000;

            let db = self.catalog.db_schema(Self::DB).unwrap();
            let table_id = db.table_definition(Self::TABLE).unwrap().table_id;
            let persisted_files = self.write_buffer.persisted_files();
            (0..count)
                .map(|i| {
                    let min_time = start + i * TEN_MINUTES;
                    let file = ParquetFile {
                        // Not `ParquetFileId::new()`: that advances the process-wide counter
                        // that other tests assert on. One id per 10 minute slot is unique.
                        id: ParquetFileId::from((min_time / TEN_MINUTES) as u64),
                        path: format!(
                            "test-host/dbs/{}-{}/{}-{}/{}/{:010}.parquet",
                            Self::DB,
                            db.id,
                            Self::TABLE,
                            table_id,
                            DateTime::<Utc>::from_timestamp_nanos(min_time)
                                .format("%Y-%m-%d/%H-%M"),
                            i
                        ),
                        size_bytes: 100,
                        row_count: 1,
                        chunk_time: min_time,
                        min_time,
                        max_time: min_time + TEN_MINUTES - 1,
                    };
                    persisted_files.add_persisted_file(&db.id, &table_id, &file);
                    file.path
                })
                .collect()
        }

        /// Write line protocol that only reaches the WAL: no snapshot persists it.
        async fn write_wal_only(&self, lp: &str) {
            use data_types::NamespaceName;

            self.write_buffer
                .write_lp(
                    NamespaceName::new(Self::DB).unwrap(),
                    lp,
                    iox_time::Time::from_timestamp_nanos(0),
                    false,
                    crate::Precision::Nanosecond,
                    false,
                )
                .await
                .unwrap();
        }

        /// Plan and run all compaction jobs through the real service code path.
        async fn compact(&self) -> Vec<ParquetFile> {
            let jobs = self.service.identify_compaction_jobs().await.unwrap();
            assert_eq!(
                jobs.len(),
                1,
                "expected exactly one gen1->gen2 job: {jobs:?}"
            );
            let job = jobs.into_iter().next().unwrap();
            assert_eq!((job.source_generation, job.target_generation), (1, 2));
            self.service
                .execute_compaction_job(job)
                .await
                .unwrap()
                .compacted_files
        }

        /// Every indexed file must exist with exactly the indexed size, and its footer must parse
        /// to the indexed row count. Then scan all indexed files the way queries do (object
        /// size taken from the index) and check the total row count.
        async fn assert_index_readable(&self, expected_rows: u64) {
            use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

            let files = self.indexed_files();
            for file in &files {
                let path = ObjPath::from(file.path.as_str());
                let meta = self.object_store.head(&path).await.unwrap();
                assert_eq!(
                    meta.size as u64, file.size_bytes,
                    "index size does not match object size for {}",
                    file.path
                );
                let bytes = self
                    .object_store
                    .get(&path)
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap();
                let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
                    .unwrap_or_else(|e| panic!("unreadable parquet {}: {e}", file.path));
                assert_eq!(
                    builder.metadata().file_metadata().num_rows() as u64,
                    file.row_count,
                    "row count mismatch for {}",
                    file.path
                );
            }

            let table_def = self
                .catalog
                .db_schema(Self::DB)
                .unwrap()
                .table_definition(Self::TABLE)
                .unwrap();
            let chunks = self
                .service
                .create_chunks_from_files(&files, &table_def.schema)
                .await
                .unwrap();
            let plan = ReorgPlanner::new()
                .compact_plan(
                    data_types::TableId::new(0),
                    Arc::clone(&table_def.table_name),
                    &table_def.schema,
                    chunks,
                    table_def.sort_key.clone(),
                )
                .unwrap();
            let ctx = {
                let mut session_config = self.service.executor.new_session_config();
                for (key, value) in &self.service.datafusion_config {
                    session_config = session_config.with_config_option(key, value);
                }
                session_config.build()
            };
            let physical = ctx.create_physical_plan(&plan).await.unwrap();
            let batches = ctx.collect(physical).await.unwrap();
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(rows as u64, expected_rows);
        }
    }

    /// Regression test: a second gen1->gen2 compaction of a window that already has a gen2 file
    /// used to reuse the name `gen2/{date}/{HH-MM}/0.parquet`, overwriting the first gen2 file
    /// while the index kept its old size ("Invalid Parquet file. Corrupt footer" on query).
    #[tokio::test]
    async fn test_recompacting_same_window_keeps_all_files_readable() {
        let h = CompactionHarness::new().await;
        // Both compactions below target the 2 minute gen2 window starting at t=0, which has ended.
        h.set_now(Duration::from_secs(120));

        // gen1 data for the 2 minute gen2 window starting at t=0.
        h.write_gen1(
            "testtable value=1 10000000000\ntesttable value=2 30000000000\n\
             testtable value=3 20000000000\ntesttable value=4 40000000000\n\
             testtable value=5 130000000000",
        )
        .await;
        let first = h.compact().await;
        assert_eq!(first.len(), 1);
        assert_eq!(
            h.gen1_file_count(),
            0,
            "gen1 inputs should be replaced by gen2"
        );
        h.assert_index_readable(5).await;

        // Late gen1 data for the same window (different row count, so a different file size).
        h.write_gen1(
            "testtable value=6 15000000000\ntesttable value=7 25000000000\n\
             testtable value=8 135000000000",
        )
        .await;
        let second = h.compact().await;
        assert_eq!(second.len(), 1);
        assert_eq!(
            first[0].chunk_time, second[0].chunk_time,
            "test setup: both compactions must target the same gen2 window"
        );
        assert_ne!(
            first[0].path, second[0].path,
            "second compaction reused the first path"
        );

        let indexed = h.indexed_files();
        let mut paths: Vec<_> = indexed.iter().map(|f| f.path.clone()).collect();
        paths.sort();
        let mut expected = vec![first[0].path.clone(), second[0].path.clone()];
        expected.sort();
        assert_eq!(
            paths, expected,
            "index should hold exactly the two gen2 files"
        );

        h.assert_index_readable(8).await;
    }

    /// Regression test (TAG-2495): a gen1 file covers at most one gen1 chunk, so the files grouped
    /// into one target window never span the whole window. Compaction used to wait for that span
    /// and never ran; it must start once the window has ended.
    #[tokio::test]
    async fn test_window_of_short_gen1_files_compacts_once_it_has_ended() {
        let h = CompactionHarness::new().await;

        // Three gen1 files in the 2 minute window [0s, 120s), together spanning only 40s.
        h.write_gen1("testtable value=1 10000000000").await;
        h.write_gen1("testtable value=2 30000000000").await;
        h.write_gen1("testtable value=3 50000000000").await;
        assert_eq!(h.gen1_file_count(), 3);

        h.set_now(Duration::from_secs(119));
        let open = h.service.identify_compaction_jobs().await.unwrap();
        assert!(open.is_empty(), "the window has not ended yet: {open:?}");

        h.set_now(Duration::from_secs(120));
        let compacted = h.compact().await;
        assert_eq!(compacted.len(), 1);
        assert_eq!(
            h.gen1_file_count(),
            0,
            "gen1 inputs should be replaced by gen2"
        );
        h.assert_index_readable(3).await;
    }

    /// 2026-09-18T00:00:00Z
    const DAY_START: i64 = 1_789_689_600_000_000_000;
    const ONE_DAY: i64 = 86_400_000_000_000;

    /// gen1 -> gen2 into 1 day windows from 10 minute gen1 files, at least 10 files a job, as on
    /// tagmobi-dev, with room for a whole day of gen1 files in one job.
    fn daily_gen2_config() -> CompactionConfig {
        CompactionConfig {
            enabled: true,
            interval: Duration::from_secs(1),
            max_files_per_run: 200,
            min_files_for_compaction: 10,
            generation_durations: HashMap::from([
                (1, Duration::from_secs(600)),
                (2, Duration::from_secs(86_400)),
            ]),
        }
    }

    /// Regression test (TAG-2495), the case from the issue: a day of 10 minute gen1 files never
    /// spans the day (the last one ends at 23:59:59.999...), so no job was ever planned. The day
    /// is one job once it has ended, and none while it is open.
    #[tokio::test]
    async fn test_day_of_ten_minute_gen1_files_compacts_once_the_day_has_ended() {
        let h = CompactionHarness::with_config(daily_gen2_config()).await;
        h.write_wal_only("testtable value=1 0").await;
        let mut day = h.index_ten_minute_gen1_files(DAY_START, 144);

        h.set_now(Duration::from_nanos((DAY_START + ONE_DAY - 1) as u64));
        let open = h.service.identify_compaction_jobs().await.unwrap();
        assert!(
            open.is_empty(),
            "the day has not ended yet: {} jobs",
            open.len()
        );

        h.set_now(Duration::from_nanos((DAY_START + ONE_DAY) as u64));
        let jobs = h.service.identify_compaction_jobs().await.unwrap();
        assert_eq!(jobs.len(), 1, "expected one job for the ended day");
        assert_eq!(
            (jobs[0].source_generation, jobs[0].target_generation),
            (1, 2)
        );
        let mut planned: Vec<_> = jobs[0].files.iter().map(|f| f.path.clone()).collect();
        planned.sort();
        day.sort();
        assert_eq!(planned, day);
    }

    /// An ended window is compacted only once it holds `min_files_for_compaction` source files.
    #[tokio::test]
    async fn test_ended_window_with_too_few_files_is_not_compacted() {
        let h = CompactionHarness::with_config(daily_gen2_config()).await;
        h.write_wal_only("testtable value=1 0").await;
        h.index_ten_minute_gen1_files(DAY_START - ONE_DAY, 9);
        let mut day = h.index_ten_minute_gen1_files(DAY_START, 144);

        // Both days have ended; only the one with at least 10 files is compacted.
        h.set_now(Duration::from_nanos((DAY_START + ONE_DAY) as u64));
        let jobs = h.service.identify_compaction_jobs().await.unwrap();
        assert_eq!(
            jobs.len(),
            1,
            "expected one job, for the day with 144 files"
        );
        let mut planned: Vec<_> = jobs[0].files.iter().map(|f| f.path.clone()).collect();
        planned.sort();
        day.sort();
        assert_eq!(planned, day);
    }

    /// Regression test (TAG-2495): startup replays the WAL after the newest snapshot's
    /// `wal_file_sequence_number`. The compaction snapshot used to record the last WAL file
    /// written there, so a restart after a compaction skipped the WAL files that had not been
    /// persisted yet, and their rows were gone.
    #[tokio::test]
    async fn test_restart_after_compaction_replays_unpersisted_wal() {
        let h = CompactionHarness::new().await;
        // One gen1 file spanning the whole window, so this test does not depend on the
        // window-has-ended fix above.
        h.write_gen1("testtable value=1 10000000000\ntesttable value=2 130000000000")
            .await;
        let persisted_wal = h.newest_snapshot().await.wal_file_sequence_number;

        h.write_wal_only("testtable value=3 140000000000").await;
        assert!(
            h.write_buffer.wal().last_wal_sequence_number().await > persisted_wal,
            "test setup: the WAL must hold a file the snapshot did not persist"
        );

        h.set_now(Duration::from_secs(120));
        h.compact().await;
        let compaction_snapshot = h.newest_snapshot().await;
        assert!(
            !compaction_snapshot.removed_files.is_empty(),
            "test setup: the newest snapshot should be the compaction's"
        );
        assert_eq!(
            compaction_snapshot.wal_file_sequence_number, persisted_wal,
            "the compaction snapshot moved the WAL replay start past an unpersisted WAL file"
        );

        // A restart must replay value=3 from the WAL; persist it to count it with the rest.
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
        assert_eq!(rows, 3, "2 compacted rows + 1 replayed from the WAL");
    }

    /// Regression test (TAG-2495): a data snapshot gets its sequence number when the WAL is
    /// flushed, and its snapshot file is written once its parquet files are. A compaction snapshot
    /// written in between got a higher number, so it became the newest, but it recorded the WAL
    /// number and `next_file_id` from before that data snapshot. A restart then replayed WAL files
    /// that were already in parquet and handed out file ids that were already used.
    #[tokio::test]
    async fn test_compaction_snapshot_waits_for_data_snapshot_in_progress() {
        use crate::paths::SnapshotInfoFilePath;
        use influxdb3_test_helpers::object_store::SynchronizedObjectStore;
        use influxdb3_wal::SnapshotSequenceNumber;
        use tokio::sync::Notify;

        // The second data snapshot is held at its snapshot file until released.
        let held = SnapshotInfoFilePath::new("test-host", SnapshotSequenceNumber::new(2));
        let reached = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let store = SynchronizedObjectStore::new(Arc::new(InMemory::new())).with_put_notifies(
            held.as_ref().clone(),
            Arc::clone(&release),
            Arc::clone(&reached),
        );
        let h = CompactionHarness::with_object_store(Arc::new(store)).await;

        // Data snapshot 1 persists the gen1 file that is compacted below.
        h.write_gen1("testtable value=1 10000000000\ntesttable value=2 130000000000")
            .await;
        // Data snapshot 2 persists value=3. Like the WAL's own flush task, hold its snapshot
        // permit until the snapshot is done, then clean up.
        h.write_wal_only("testtable value=3 140000000000").await;
        let wal = h.write_buffer.wal();
        let (snapshot_done, data_snapshot, permit) = wal
            .force_flush_buffer()
            .await
            .expect("test setup: no data snapshot");
        assert_eq!(
            data_snapshot.snapshot_sequence_number,
            SnapshotSequenceNumber::new(2),
            "test setup: the held snapshot file must be this snapshot's"
        );
        let snapshot_task = tokio::spawn({
            let wal = Arc::clone(&wal);
            async move {
                snapshot_done.await.expect("snapshot failed");
                wal.cleanup_snapshot(data_snapshot, permit).await;
            }
        });
        reached.notified().await;

        // Compact while data snapshot 2 is still being written. Run the compaction until it has
        // reserved its snapshot number (planning and the merge are done by then), then give it
        // time to write its snapshot, as it did before it waited for data snapshot 2.
        h.set_now(Duration::from_secs(120));
        let compaction = h.compact();
        tokio::pin!(compaction);
        let mut compacted_first = false;
        let mut polls = 0;
        while wal.last_snapshot_sequence_number().await < SnapshotSequenceNumber::new(3) {
            if tokio::time::timeout(Duration::from_millis(50), &mut compaction)
                .await
                .is_ok()
            {
                compacted_first = true;
                break;
            }
            polls += 1;
            assert!(
                polls < 600,
                "the compaction never reserved its snapshot number"
            );
        }
        if !compacted_first {
            compacted_first = tokio::time::timeout(Duration::from_millis(500), &mut compaction)
                .await
                .is_ok();
        }
        release.notify_one();
        if !compacted_first {
            compaction.await;
        }
        snapshot_task.await.unwrap();

        assert!(
            !compacted_first,
            "the compaction snapshot was written while data snapshot 2 was still being written"
        );
        assert_eq!(
            h.newest_snapshot().await.wal_file_sequence_number,
            data_snapshot.last_wal_sequence_number,
            "the newest snapshot must record data snapshot 2's WAL number"
        );
    }

    /// Regression test (TAG-2495): a data snapshot with nothing to write writes no snapshot file,
    /// but the WAL still treats its files as snapshotted and deletes them. A compaction snapshot
    /// after it must record its WAL number: an older one would make a restart reuse the numbers
    /// of deleted WAL files.
    #[tokio::test]
    async fn test_compaction_snapshot_records_data_snapshots_that_wrote_nothing() {
        let h = CompactionHarness::new().await;
        h.write_gen1("testtable value=1 10000000000\ntesttable value=2 130000000000")
            .await;
        let persisted_wal = h.newest_snapshot().await.wal_file_sequence_number;

        // The buffer is empty, so this snapshot persists nothing.
        let (snapshot_done, empty_snapshot, permit) = h
            .write_buffer
            .wal()
            .force_flush_buffer()
            .await
            .expect("test setup: no data snapshot");
        snapshot_done.await.expect("snapshot failed");
        drop(permit);
        assert!(
            empty_snapshot.last_wal_sequence_number > persisted_wal,
            "test setup: the empty snapshot must cover a later WAL file"
        );
        assert_eq!(
            h.newest_snapshot().await.wal_file_sequence_number,
            persisted_wal,
            "test setup: the empty snapshot must not write a snapshot file"
        );

        h.set_now(Duration::from_secs(120));
        h.compact().await;
        assert_eq!(
            h.newest_snapshot().await.wal_file_sequence_number,
            empty_snapshot.last_wal_sequence_number
        );
    }

    /// Regression test (TAG-2495): the compaction snapshot's upload retries until it succeeds.
    /// Holding the WAL's snapshot permit through it made a flush that starts a data snapshot,
    /// and so every write, wait for as long as the object store failed.
    #[tokio::test]
    async fn test_flush_does_not_wait_for_compaction_snapshot_upload() {
        use crate::paths::SnapshotInfoFilePath;
        use influxdb3_test_helpers::object_store::SynchronizedObjectStore;
        use influxdb3_wal::SnapshotSequenceNumber;
        use tokio::sync::Notify;

        // Data snapshot 1 persists the gen1 file; the compaction snapshot is 2 and is held.
        let held = SnapshotInfoFilePath::new("test-host", SnapshotSequenceNumber::new(2));
        let reached = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let store = SynchronizedObjectStore::new(Arc::new(InMemory::new())).with_put_notifies(
            held.as_ref().clone(),
            Arc::clone(&release),
            Arc::clone(&reached),
        );
        let h = CompactionHarness::with_object_store(Arc::new(store)).await;
        h.write_gen1("testtable value=1 10000000000\ntesttable value=2 130000000000")
            .await;

        h.set_now(Duration::from_secs(120));
        let compaction = h.compact();
        tokio::pin!(compaction);
        tokio::select! {
            _ = &mut compaction => panic!("test setup: the compaction snapshot was not held"),
            _ = reached.notified() => {}
        }

        let flushed = tokio::time::timeout(
            Duration::from_secs(1),
            h.write_buffer.wal().force_flush_buffer(),
        )
        .await;
        release.notify_one();
        compaction.await;

        let (snapshot_done, _, permit) = flushed
            .expect("the flush waited for the compaction snapshot upload")
            .expect("test setup: no data snapshot");
        snapshot_done.await.expect("snapshot failed");
        drop(permit);
    }

    /// If writing a later output fails, outputs already uploaded by this attempt are deleted.
    #[tokio::test]
    async fn test_failed_compaction_write_deletes_uploaded_outputs() {
        use arrow::array::{Float64Array, Int64Array, TimestampNanosecondArray};
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use arrow::record_batch::RecordBatch;
        use futures::TryStreamExt;

        let h = CompactionHarness::new().await;
        let schema = schema::SchemaBuilder::new()
            .influx_field("value", schema::InfluxFieldType::Float)
            .timestamp()
            .build()
            .unwrap();
        let job = CompactionJob {
            database_id: DbId::from(0),
            database_name: Arc::from(CompactionHarness::DB),
            table_id: TableId::from(0),
            table_name: Arc::from(CompactionHarness::TABLE),
            source_generation: 1,
            target_generation: 2,
            files: vec![],
            schema: schema.clone(),
            sort_key: SortKey::from_columns(vec!["time"]),
        };

        let good_columns = schema
            .as_arrow()
            .fields()
            .iter()
            .map(|f| match f.name().as_str() {
                "value" => Arc::new(Float64Array::from(vec![1.0, 2.0])) as arrow::array::ArrayRef,
                "time" => Arc::new(TimestampNanosecondArray::from(vec![
                    10_000_000_000,
                    20_000_000_000,
                ])),
                other => panic!("unexpected column {other}"),
            })
            .collect();
        let good = RecordBatch::try_new(schema.as_arrow(), good_columns).unwrap();
        // A `time` column that is not a timestamp makes the second output fail after the
        // first one has already been uploaded.
        let bad = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                Field::new("value", DataType::Float64, true),
                Field::new("time", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Float64Array::from(vec![3.0])),
                Arc::new(Int64Array::from(vec![30_000_000_000])),
            ],
        )
        .unwrap();

        // Sanity check: the good batch alone uploads one object.
        let ok = h
            .service
            .write_compacted_files(&job, vec![good.clone()], &schema)
            .await
            .unwrap();
        assert_eq!(ok.len(), 1);
        h.service
            .delete_unreferenced_outputs(&[ObjPath::from(ok[0].path.as_str())])
            .await;

        let err = h
            .service
            .write_compacted_files(&job, vec![good, bad], &schema)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not a timestamp"), "{err:#}");

        let leftover: Vec<_> = h
            .object_store
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.location.to_string())
            .filter(|p| p.contains("/gen2/"))
            .collect();
        assert!(leftover.is_empty(), "orphaned outputs left behind: {leftover:?}");
    }

    #[tokio::test]
    async fn test_compaction_sorts_and_updates_metadata() {
        use crate::{ParquetFile, ParquetFileId};

        // Create a fake ParquetFile for input
        let input_file = ParquetFile {
            id: ParquetFileId::new(),
            path: "dbs/test-1/test-1/gen1/2023-01-01/00-00/0.parquet".to_string(),
            size_bytes: 123,
            row_count: 3,
            chunk_time: 0,
            min_time: 100,
            max_time: 300,
        };

        // Test basic file properties
        assert_eq!(input_file.min_time, 100);
        assert_eq!(input_file.max_time, 300);
        assert_eq!(input_file.size_bytes, 123);
        assert_eq!(input_file.row_count, 3);
        
        println!("✅ Mock compaction test passed! File properties verified");
    }

    #[tokio::test]
    async fn test_minimal_line_protocol_write() {
        use std::sync::Arc;
        use object_store::memory::InMemory;
        use influxdb3_catalog::catalog::Catalog;
        use data_types::NamespaceName;
        use crate::write_buffer::{WriteBufferImpl, WriteBufferImplArgs};
        use crate::persister::Persister;
        use influxdb3_cache::last_cache::LastCacheProvider;
        use influxdb3_cache::distinct_cache::DistinctCacheProvider;
        use iox_query::exec::Executor;
        use iox_time::MockProvider;
        use influxdb3_wal::WalConfig;
        use metric::Registry;
        use influxdb3_shutdown::ShutdownManager;
        use crate::Precision;

        // Set up in-memory object store and catalog
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let time_provider: Arc<dyn iox_time::TimeProvider> = Arc::new(MockProvider::new(iox_time::Time::from_timestamp_nanos(0)));
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

        // Create database
        let db_name = "testdb";
        catalog.create_database(db_name).await.unwrap();

        // Set up write buffer
        let persister = Arc::new(Persister::new(
            Arc::clone(&object_store),
            "test-host",
            Arc::clone(&time_provider),
        ));
        let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog)).await.unwrap();
        let distinct_cache = DistinctCacheProvider::new_from_catalog(
            Arc::clone(&time_provider),
            Arc::clone(&catalog),
        )
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

        // Write simple line protocol
        let lp = "testtable value=1 10";
        let result = write_buffer
            .write_lp(
                NamespaceName::new(db_name).unwrap(),
                lp,
                iox_time::Time::from_timestamp_nanos(0),
                false,
                Precision::Nanosecond,
                false,
            )
            .await;

        match result {
            Ok(_) => println!("✅ Line protocol write succeeded!"),
            Err(e) => {
                println!("❌ Line protocol write failed: {:?}", e);
                panic!("Line protocol write failed: {:?}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_real_compaction_sorts_and_updates_metadata() {
        use std::sync::Arc;
        use std::collections::HashMap;
        use std::time::Duration;
        use object_store::memory::InMemory;
        use influxdb3_catalog::catalog::Catalog;
        use data_types::NamespaceName;
        use crate::write_buffer::{WriteBufferImpl, WriteBufferImplArgs};
        use crate::persister::Persister;
        use influxdb3_cache::last_cache::LastCacheProvider;
        use influxdb3_cache::distinct_cache::DistinctCacheProvider;
        use iox_query::exec::Executor;
        use influxdb3_wal::WalConfig;
        use metric::Registry;
        use crate::compaction::{CompactionConfig, CompactionService};
        use influxdb3_shutdown::ShutdownManager;
        use crate::Precision;
        use crate::persister::DEFAULT_OBJECT_STORE_URL;
        use datafusion::execution::object_store::ObjectStoreUrl;
        use iox_time::MockProvider;

        // Set up in-memory object store and catalog
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let clock = Arc::new(MockProvider::new(iox_time::Time::from_timestamp_nanos(0)));
        let time_provider: Arc<dyn iox_time::TimeProvider> = Arc::clone(&clock) as _;
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

        // Create table in catalog
        let db_name = "testdb";
        let table_name = "testtable";
        catalog.create_database(db_name).await.unwrap();
        // Let the line protocol write create the table automatically
        let db_id = catalog.db_name_to_id(db_name).unwrap();
        // We'll get the table_id after the line protocol write creates the table

        // Set up write buffer
        let persister = Arc::new(Persister::new(
            Arc::clone(&object_store),
            "test-host",
            Arc::clone(&time_provider),
        ));
        let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog)).await.unwrap();
        let distinct_cache = DistinctCacheProvider::new_from_catalog(
            Arc::clone(&time_provider),
            Arc::clone(&catalog),
        )
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

        // Write unsorted data using line protocol (this is how real data comes in)
        let unsorted_lp = "testtable value=1 10000000000\ntesttable value=2 30000000000\ntesttable value=3 20000000000\ntesttable value=4 40000000000\ntesttable value=5 130000000000";
        let _ = write_buffer
            .write_lp(
                NamespaceName::new(db_name).unwrap(),
                unsorted_lp,
                iox_time::Time::from_timestamp_nanos(0),
                false,
                Precision::Nanosecond,
                false,
            )
            .await
            .unwrap();

        // Force a snapshot to persist the data
        let _ = write_buffer.wal().force_flush_buffer().await;
        
        // Get the table_id after the line protocol write creates the table
        let table_id = catalog.db_schema(db_name).unwrap().table_definition(table_name).unwrap().table_id;
        
        // Wait for the data to be persisted. Persisting runs in the background, so poll instead
        // of sleeping a fixed amount (which is flaky when other tests load the machine).
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        let files = loop {
            let files = write_buffer.persisted_files().get_files(db_id, table_id);
            if !files.is_empty() || std::time::Instant::now() >= deadline {
                break files;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        assert!(!files.is_empty(), "Should have persisted files before compaction");

        // Set up compaction service
        let mut generation_durations = HashMap::new();
        generation_durations.insert(1, Duration::from_secs(60)); // 1 minute
        generation_durations.insert(2, Duration::from_secs(120)); // 2 minutes
        
        let compaction_config = CompactionConfig {
            enabled: true,
            interval: Duration::from_secs(1),
            max_files_per_run: 10,
            min_files_for_compaction: 1,
            generation_durations,
        };

        // Use .clone() so we can use write_buffer later
        let compaction_service = CompactionService::new(
            compaction_config,
            Arc::clone(&catalog),
            write_buffer.clone(),
            write_buffer.persisted_files(),
            Arc::clone(&persister),
            Arc::new(Executor::new_testing()),
            HashMap::new(),
            Arc::clone(&object_store),
            ObjectStoreUrl::parse(DEFAULT_OBJECT_STORE_URL).unwrap(),
            "test-host",
            Arc::clone(&time_provider),
            ShutdownManager::new_testing().register(),
        );

        // Windows are compacted once they have ended: move past the 2 minute window [0s, 120s).
        clock.set(iox_time::Time::from_timestamp_nanos(120_000_000_000));

        // Run compaction cycle to identify and execute compaction jobs
        let jobs = compaction_service.identify_compaction_jobs().await.unwrap();
        assert!(!jobs.is_empty(), "Should have identified compaction jobs");
        
        // Verify the job properties
        let job = &jobs[0];
        assert_eq!(job.database_id, db_id);
        assert_eq!(job.table_id, table_id);
        assert_eq!(job.table_name.as_ref(), table_name);
        assert_eq!(job.source_generation, 1);
        assert_eq!(job.target_generation, 2);
        assert!(!job.files.is_empty(), "Job should have files to compact");
        
        println!("✅ Compaction test passed! Successfully identified {} compaction jobs", jobs.len());
        println!("   - Database: {}", job.database_id);
        println!("   - Table: {}", job.table_name);
        println!("   - Generation: {} -> {}", job.source_generation, job.target_generation);
        println!("   - Files: {}", job.files.len());
    }
} 