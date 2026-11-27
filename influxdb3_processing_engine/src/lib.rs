use crate::environment::PythonEnvironmentManager;
use crate::manager::ProcessingEngineError;

use crate::plugins::PluginContext;
use crate::plugins::{PluginError, ProcessingEngineEnvironmentManager};
use anyhow::Context;
use bytes::Bytes;
use hashbrown::HashMap;
use influxdb3_catalog::CatalogError;
use influxdb3_catalog::catalog::Catalog;
use influxdb3_catalog::channel::CatalogUpdateReceiver;
use influxdb3_catalog::log::{
    CatalogBatch, DatabaseCatalogOp, DeleteTriggerLog, PluginType, TriggerDefinition,
    TriggerIdentifier, TriggerSpecificationDefinition, ValidPluginFilename,
};
use influxdb3_id::{DbId, TriggerId};
use influxdb3_internal_api::query_executor::QueryExecutor;
use influxdb3_py_api::system_py::{CacheStore, PyCache};
use influxdb3_sys_events::SysEventStore;
use influxdb3_types::http::{
    SchedulePluginTestRequest, SchedulePluginTestResponse, WalPluginTestRequest,
    WalPluginTestResponse,
};
use influxdb3_wal::{SnapshotDetails, WalContents, WalFileNotifier};
use influxdb3_write::WriteBuffer;
use iox_http_util::Response;
use iox_time::TimeProvider;
use observability_deps::tracing::{debug, error, warn};
use parking_lot::Mutex;
use std::any::Any;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::oneshot::Receiver;
use tokio::sync::{RwLock, mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout_at};

pub mod environment;
pub mod manager;
pub mod plugins;

pub mod virtualenv;

#[derive(Debug)]
pub struct ProcessingEngineManagerImpl {
    environment_manager: ProcessingEngineEnvironmentManager,
    catalog: Arc<Catalog>,
    node_id: Arc<str>,
    write_buffer: Arc<dyn WriteBuffer>,
    query_executor: Arc<dyn QueryExecutor>,
    time_provider: Arc<dyn TimeProvider>,
    sys_event_store: Arc<SysEventStore>,
    cache: Arc<Mutex<CacheStore>>,
    plugin_event_tx: RwLock<PluginChannels>,
}

type TriggerKey = (DbId, TriggerId);

#[derive(Debug, Default)]
struct PluginChannels {
    running: HashMap<TriggerKey, RunningTrigger>,
    request_paths: HashMap<String, TriggerKey>,
}

#[derive(Debug)]
struct RunningTrigger {
    database_name: Arc<str>,
    trigger_name: Arc<str>,
    route: TriggerRoute,
    cancellation: watch::Sender<bool>,
    cache: PyCache,
    task: JoinHandle<()>,
}

#[derive(Debug)]
enum TriggerRoute {
    Wal(mpsc::Sender<WalEvent>),
    Schedule,
    Request(String, mpsc::Sender<Request>),
}

impl TriggerRoute {
    fn plugin_type(&self) -> PluginType {
        match self {
            Self::Wal(_) => PluginType::WalRows,
            Self::Schedule => PluginType::Schedule,
            Self::Request(..) => PluginType::Request,
        }
    }
}

const PLUGIN_EVENT_BUFFER_SIZE: usize = 60;
const SHUTDOWN_BUDGET: Duration = Duration::from_secs(20);

impl PluginChannels {
    fn insert(&mut self, key: TriggerKey, runtime: RunningTrigger) {
        if let TriggerRoute::Request(path, _) = &runtime.route {
            self.request_paths.insert(path.clone(), key);
        }
        self.running.insert(key, runtime);
    }

    fn detach(
        &mut self,
        key: TriggerKey,
        generation: Option<&watch::Receiver<bool>>,
    ) -> Option<RunningTrigger> {
        let runtime = self.running.get(&key)?;
        if generation
            .is_some_and(|generation| !runtime.cancellation.subscribe().same_channel(generation))
        {
            return None;
        }

        let runtime = self.running.remove(&key)?;
        if let TriggerRoute::Request(path, _) = &runtime.route {
            if self.request_paths.get(path) == Some(&key) {
                self.request_paths.remove(path);
            }
        }
        Some(runtime)
    }
}

async fn cancelled(receiver: &mut watch::Receiver<bool>) {
    // A closed control channel also has no running owner.
    let _ = receiver.wait_for(|cancelled| *cancelled).await;
}

async fn send_to_trigger<T>(
    sender: mpsc::Sender<T>,
    mut cancellation: watch::Receiver<bool>,
    event: T,
) -> bool {
    tokio::select! {
        biased;
        _ = cancelled(&mut cancellation) => false,
        result = sender.send(event) => result.is_ok(),
    }
}

async fn wait_for_shutdown(
    runtime: RunningTrigger,
    (database_id, trigger_id): TriggerKey,
    operation: &'static str,
    started: Instant,
) {
    let mut task = runtime.task;
    if timeout_at(started + SHUTDOWN_BUDGET, &mut task)
        .await
        .is_err()
    {
        task.abort();
        warn!(
            operation,
            ?database_id,
            ?trigger_id,
            database = %runtime.database_name,
            trigger = %runtime.trigger_name,
            plugin_type = ?runtime.route.plugin_type(),
            phase = "completion",
            budget_ms = SHUTDOWN_BUDGET.as_millis() as u64,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "trigger shutdown timed out"
        );
    }
}

impl ProcessingEngineManagerImpl {
    pub async fn new(
        environment: ProcessingEngineEnvironmentManager,
        catalog: Arc<Catalog>,
        node_id: impl Into<Arc<str>>,
        write_buffer: Arc<dyn WriteBuffer>,
        query_executor: Arc<dyn QueryExecutor>,
        time_provider: Arc<dyn TimeProvider>,
        sys_event_store: Arc<SysEventStore>,
    ) -> Arc<Self> {
        // if given a plugin dir, try to initialize the virtualenv.
        if let Some(plugin_dir) = &environment.plugin_dir {
            {
                environment
                    .package_manager
                    .init_pyenv(plugin_dir, environment.virtual_env_location.as_ref())
                    .expect("unable to initialize python environment");
                virtualenv::init_pyo3();
            }
        }

        let catalog_sub = catalog.subscribe_to_updates("processing_engine").await;

        let cache = Arc::new(Mutex::new(CacheStore::new(
            Arc::clone(&time_provider),
            Duration::from_secs(10),
        )));

        let pem = Arc::new(Self {
            environment_manager: environment,
            catalog,
            node_id: node_id.into(),
            write_buffer,
            query_executor,
            sys_event_store,
            time_provider,
            plugin_event_tx: Default::default(),
            cache,
        });

        background_catalog_update(Arc::clone(&pem), catalog_sub);

        pem
    }

    pub fn node_id(&self) -> Arc<str> {
        Arc::clone(&self.node_id)
    }

    pub async fn validate_plugin_filename<'a>(
        &self,
        name: &'a str,
    ) -> Result<ValidPluginFilename<'a>, PluginError> {
        let _ = self.read_plugin_code(name).await?;
        Ok(ValidPluginFilename::from_validated_name(name))
    }

    pub async fn read_plugin_code(&self, name: &str) -> Result<PluginCode, PluginError> {
        // if the name starts with gh: then we need to get it from the public github repo at https://github.com/influxdata/influxdb3_plugins/tree/main
        if name.starts_with("gh:") {
            let plugin_path = name.strip_prefix("gh:").unwrap();
            let url = format!(
                "https://raw.githubusercontent.com/influxdata/influxdb3_plugins/main/{plugin_path}"
            );
            let resp = reqwest::get(&url)
                .await
                .context("error getting plugin from github repo")?;

            // verify the response is a success
            if !resp.status().is_success() {
                return Err(PluginError::FetchingFromGithub(resp.status(), url));
            }

            let resp_body = resp
                .text()
                .await
                .context("error reading plugin from github repo")?;
            return Ok(PluginCode::Github(Arc::from(resp_body)));
        }

        // otherwise we assume it is a local file
        let plugin_dir = self
            .environment_manager
            .plugin_dir
            .clone()
            .ok_or(PluginError::NoPluginDir)?;
        let plugin_path = plugin_dir.join(name);

        // read it at least once to make sure it's there
        let code = std::fs::read_to_string(plugin_path.clone())?;

        // now we can return it
        Ok(PluginCode::Local(LocalPlugin {
            plugin_path,
            last_read_and_code: Mutex::new((SystemTime::now(), Arc::from(code))),
        }))
    }
}

#[derive(Debug)]
pub enum PluginCode {
    Github(Arc<str>),
    Local(LocalPlugin),
}

impl PluginCode {
    pub(crate) fn code(&self) -> Arc<str> {
        match self {
            PluginCode::Github(code) => Arc::clone(code),
            PluginCode::Local(plugin) => plugin.read_if_modified(),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct LocalPlugin {
    plugin_path: PathBuf,
    last_read_and_code: Mutex<(SystemTime, Arc<str>)>,
}

impl LocalPlugin {
    fn read_if_modified(&self) -> Arc<str> {
        let metadata = std::fs::metadata(&self.plugin_path);

        let mut last_read_and_code = self.last_read_and_code.lock();
        let (last_read, code) = &mut *last_read_and_code;

        match metadata {
            Ok(metadata) => {
                let is_modified = match metadata.modified() {
                    Ok(modified) => modified > *last_read,
                    Err(_) => true, // if we can't get the modified time, assume it is modified
                };

                if is_modified {
                    // attempt to read the code, if it fails we will return the last known code
                    if let Ok(new_code) = std::fs::read_to_string(&self.plugin_path) {
                        *last_read = SystemTime::now();
                        *code = Arc::from(new_code);
                    } else {
                        error!("error reading plugin file {:?}", self.plugin_path);
                    }
                }

                Arc::clone(code)
            }
            Err(_) => Arc::clone(code),
        }
    }
}

impl ProcessingEngineManagerImpl {
    async fn run_trigger(
        self: Arc<Self>,
        db_name: &str,
        trigger_name: &str,
        expected: Option<TriggerKey>,
    ) -> Result<(), ProcessingEngineError> {
        debug!(db_name, trigger_name, "starting trigger");
        let db_schema = self
            .catalog
            .db_schema(db_name)
            .ok_or_else(|| ProcessingEngineError::DatabaseNotFound(db_name.to_string()))?;
        let trigger = db_schema
            .processing_engine_triggers
            .get_by_name(trigger_name)
            .ok_or_else(|| CatalogError::ProcessingEngineTriggerNotFound {
                database_name: db_name.to_string(),
                trigger_name: trigger_name.to_string(),
            })?;
        let key = (db_schema.id, trigger.trigger_id);
        if trigger.disabled || expected.is_some_and(|expected| expected != key) {
            return Ok(());
        }
        if trigger.node_id != self.node_id {
            error!(
                trigger_name, node_id = %trigger.node_id,
                "trigger is configured for a different node"
            );
            return Ok(());
        }

        let plugin_code = Arc::new(self.read_plugin_code(&trigger.plugin_filename).await?);
        let mut channels = self.plugin_event_tx.write().await;
        // Startup can race an update while loading the plugin file.
        let still_active = self.catalog.db_schema(db_name).is_some_and(|db| {
            db.id == key.0
                && db
                    .processing_engine_triggers
                    .get_by_name(trigger_name)
                    .is_some_and(|current| current.trigger_id == key.1 && !current.disabled)
        });
        if !still_active || channels.running.contains_key(&key) {
            return Ok(());
        }

        let (cancellation, receiver) = watch::channel(false);
        let cache = PyCache::new_trigger_cache(Arc::clone(&self.cache), receiver.clone());
        let context = PluginContext {
            write_buffer: Arc::clone(&self.write_buffer),
            query_executor: Arc::clone(&self.query_executor),
            sys_event_store: Arc::clone(&self.sys_event_store),
            manager: Arc::clone(&self),
            key,
            cancellation: receiver,
            cache: cache.clone(),
        };
        let (route, task) = match trigger.trigger.plugin_type() {
            PluginType::Request => {
                let TriggerSpecificationDefinition::RequestPath { path } = &trigger.trigger else {
                    unreachable!()
                };
                let (tx, rx) = mpsc::channel(PLUGIN_EVENT_BUFFER_SIZE);
                let task = plugins::run_request_plugin(
                    db_name.to_string(),
                    plugin_code,
                    Arc::clone(&trigger),
                    context,
                    rx,
                );
                (TriggerRoute::Request(path.clone(), tx), task)
            }
            PluginType::WalRows => {
                let (tx, rx) = mpsc::channel(PLUGIN_EVENT_BUFFER_SIZE);
                let task = plugins::run_wal_contents_plugin(
                    db_name.to_string(),
                    plugin_code,
                    Arc::clone(&trigger),
                    context,
                    rx,
                );
                (TriggerRoute::Wal(tx), task)
            }
            PluginType::Schedule => {
                let task = plugins::run_schedule_plugin(
                    db_name.to_string(),
                    plugin_code,
                    Arc::clone(&trigger),
                    Arc::clone(&self.time_provider),
                    context,
                )?;
                (TriggerRoute::Schedule, task)
            }
        };
        channels.insert(
            key,
            RunningTrigger {
                database_name: Arc::clone(&trigger.database_name),
                trigger_name: Arc::clone(&trigger.trigger_name),
                route,
                cancellation,
                cache,
                task,
            },
        );
        Ok(())
    }

    async fn stop_trigger(
        &self,
        key: TriggerKey,
        generation: Option<&watch::Receiver<bool>>,
        operation: &'static str,
    ) -> Option<JoinHandle<()>> {
        let mut channels = self.plugin_event_tx.write().await;
        let runtime = channels.detach(key, generation)?;
        let started = Instant::now();
        // ponytail: A watch signal dispatches synchronously, even with a full event queue.
        runtime.cancellation.send_replace(true);
        drop(channels);
        // The committed stop waits for the runtime to exit. Cache reclamation is independent.
        drop(runtime.cache.retire_trigger_cache());
        Some(tokio::spawn(wait_for_shutdown(
            runtime, key, operation, started,
        )))
    }

    pub async fn start_triggers(self: Arc<Self>) -> Result<(), ProcessingEngineError> {
        let triggers = self.catalog.active_triggers();
        for (db_name, trigger_name) in triggers {
            Arc::clone(&self)
                .run_trigger(&db_name, &trigger_name, None)
                .await?;
        }
        Ok(())
    }

    pub async fn test_wal_plugin(
        &self,
        request: WalPluginTestRequest,
        query_executor: Arc<dyn QueryExecutor>,
    ) -> Result<WalPluginTestResponse, plugins::PluginError> {
        {
            let catalog = Arc::clone(&self.catalog);
            let now = self.time_provider.now();

            let code = self.read_plugin_code(&request.filename).await?;
            let cache = Arc::clone(&self.cache);
            let code_string = code.code().to_string();

            let res = tokio::task::spawn_blocking(move || {
                plugins::run_test_wal_plugin(
                    now,
                    catalog,
                    query_executor,
                    code_string,
                    cache,
                    request,
                )
                .unwrap_or_else(|e| WalPluginTestResponse {
                    log_lines: vec![],
                    database_writes: Default::default(),
                    errors: vec![e.to_string()],
                })
            })
            .await?;

            Ok(res)
        }
    }

    pub async fn test_schedule_plugin(
        &self,
        request: SchedulePluginTestRequest,
        query_executor: Arc<dyn QueryExecutor>,
    ) -> Result<SchedulePluginTestResponse, PluginError> {
        {
            let catalog = Arc::clone(&self.catalog);
            let now = self.time_provider.now();

            let code = self.read_plugin_code(&request.filename).await?;
            let code_string = code.code().to_string();
            let cache = Arc::clone(&self.cache);

            let res = tokio::task::spawn_blocking(move || {
                plugins::run_test_schedule_plugin(
                    now,
                    catalog,
                    query_executor,
                    code_string,
                    cache,
                    request,
                )
            })
            .await?
            .unwrap_or_else(|e| SchedulePluginTestResponse {
                log_lines: vec![],
                database_writes: Default::default(),
                errors: vec![e.to_string()],
                trigger_time: None,
            });

            Ok(res)
        }
    }

    pub async fn request_trigger(
        &self,
        trigger_path: &str,
        query_params: HashMap<String, String>,
        request_headers: HashMap<String, String>,
        request_body: Bytes,
    ) -> Result<Response, ProcessingEngineError> {
        // oneshot channel for the response
        let (tx, rx) = oneshot::channel();
        let request = Request {
            query_params,
            headers: request_headers,
            body: request_body,
            response_tx: tx,
        };

        let route = {
            let channels = self.plugin_event_tx.read().await;
            channels.request_paths.get(trigger_path).and_then(|key| {
                let runtime = channels.running.get(key)?;
                let TriggerRoute::Request(_, sender) = &runtime.route else {
                    return None;
                };
                Some((sender.clone(), runtime.cancellation.subscribe()))
            })
        };
        let (sender, cancellation) = route.ok_or(ProcessingEngineError::RequestTriggerNotFound)?;
        if !send_to_trigger(sender, cancellation, request).await {
            return Err(ProcessingEngineError::RequestTriggerNotFound);
        }

        rx.await.map_err(|e| {
            error!(%e, "error receiving response from plugin");
            ProcessingEngineError::RequestHandlerDown
        })
    }

    pub fn get_environment_manager(&self) -> Arc<dyn PythonEnvironmentManager> {
        Arc::clone(&self.environment_manager.package_manager)
    }
}

#[async_trait::async_trait]
impl WalFileNotifier for ProcessingEngineManagerImpl {
    async fn notify(&self, write: Arc<WalContents>) {
        let routes: Vec<_> = self
            .plugin_event_tx
            .read()
            .await
            .running
            .values()
            .filter_map(|runtime| match &runtime.route {
                TriggerRoute::Wal(sender) => Some((
                    sender.clone(),
                    runtime.cancellation.subscribe(),
                    Arc::clone(&runtime.database_name),
                    Arc::clone(&runtime.trigger_name),
                )),
                _ => None,
            })
            .collect();
        for (sender, cancellation, db, trigger) in routes {
            if !send_to_trigger(
                sender,
                cancellation.clone(),
                WalEvent::WriteWalContents(Arc::clone(&write)),
            )
            .await
                && !*cancellation.borrow()
            {
                warn!(%db, %trigger, "error sending wal contents to plugin");
            }
        }
    }

    async fn notify_and_snapshot(
        &self,
        write: Arc<WalContents>,
        snapshot_details: SnapshotDetails,
    ) -> Receiver<SnapshotDetails> {
        self.notify(write).await;

        // configure a reciever that we immediately close
        let (tx, rx) = oneshot::channel();
        tx.send(snapshot_details).ok();
        rx
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[allow(dead_code)]
pub(crate) enum WalEvent {
    WriteWalContents(Arc<WalContents>),
}

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct Request {
    pub query_params: HashMap<String, String>,
    pub headers: HashMap<String, String>,
    pub body: Bytes,
    pub response_tx: oneshot::Sender<Response>,
}

fn background_catalog_update(
    processing_engine_manager: Arc<ProcessingEngineManagerImpl>,
    mut subscription: CatalogUpdateReceiver,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(mut catalog_update) = subscription.recv().await {
            let mut completions = Vec::new();
            for batch in catalog_update
                .batches()
                .filter_map(CatalogBatch::as_database)
            {
                for op in batch.ops.iter() {
                    let processing_engine_manager = Arc::clone(&processing_engine_manager);
                    match op {
                        DatabaseCatalogOp::CreateTrigger(TriggerDefinition {
                            trigger_name,
                            database_name,
                            trigger_id,
                            disabled,
                            ..
                        }) => {
                            if !disabled {
                                if let Err(error) = processing_engine_manager
                                    .run_trigger(
                                        database_name,
                                        trigger_name,
                                        Some((batch.database_id, *trigger_id)),
                                    )
                                    .await
                                {
                                    error!(?error, "failed to run the created trigger");
                                }
                            }
                        }
                        DatabaseCatalogOp::EnableTrigger(TriggerIdentifier {
                            db_id,
                            db_name,
                            trigger_id,
                            trigger_name,
                        }) => {
                            if let Err(error) = processing_engine_manager
                                .run_trigger(db_name, trigger_name, Some((*db_id, *trigger_id)))
                                .await
                            {
                                error!(?error, "failed to run the trigger");
                            }
                        }
                        DatabaseCatalogOp::DeleteTrigger(DeleteTriggerLog {
                            trigger_id,
                            force: true,
                            ..
                        }) => {
                            completions.extend(
                                processing_engine_manager
                                    .stop_trigger(
                                        (batch.database_id, *trigger_id),
                                        None,
                                        "force_delete",
                                    )
                                    .await,
                            );
                        }
                        DatabaseCatalogOp::DisableTrigger(TriggerIdentifier {
                            db_id,
                            trigger_id,
                            ..
                        }) => {
                            completions.extend(
                                processing_engine_manager
                                    .stop_trigger((*db_id, *trigger_id), None, "disable")
                                    .await,
                            );
                        }
                        // NOTE(trevor/catalog-refactor): it is not clear that any other operation needs to be
                        // handled, based on the existing code, but we could potentially
                        // handle database deletion, trigger creation/deletion/enable here
                        _ => (),
                    }
                }
            }
            catalog_update.defer_completion(completions);
        }
    })
}

#[cfg(test)]
mod tests {
    use crate::environment::DisabledManager;
    use crate::plugins::ProcessingEngineEnvironmentManager;
    use crate::{
        ProcessingEngineManagerImpl, Request, RunningTrigger, SHUTDOWN_BUDGET, TriggerKey,
        TriggerRoute,
    };
    use arrow_array::{Array, StringArray};
    use bytes::Bytes;
    use data_types::NamespaceName;
    use datafusion_util::config::register_iox_object_store;
    use hashbrown::HashMap;
    use influxdb3_cache::distinct_cache::DistinctCacheProvider;
    use influxdb3_cache::last_cache::LastCacheProvider;
    use influxdb3_catalog::CatalogError;
    use influxdb3_catalog::catalog::Catalog;
    use influxdb3_catalog::log::{ErrorBehavior, TriggerSettings, TriggerSpecificationDefinition};
    use influxdb3_id::{ColumnId, SerdeVecMap};
    use influxdb3_internal_api::query_executor::UnimplementedQueryExecutor;
    use influxdb3_py_api::logging::ProcessingEngineLog;
    use influxdb3_py_api::system_py::PyCache;
    use influxdb3_shutdown::ShutdownManager;
    use influxdb3_sys_events::SysEventStore;
    use influxdb3_wal::{
        Field, FieldData, Gen1Duration, Row, TableChunk, TableChunks, WalConfig, WalContents,
        WalFileNotifier, WalFileSequenceNumber, WalOp, WriteBatch,
    };
    use influxdb3_write::persister::Persister;
    use influxdb3_write::write_buffer::{
        N_SNAPSHOTS_TO_LOAD_ON_START, WriteBufferImpl, WriteBufferImplArgs,
    };
    use influxdb3_write::{Precision, WriteBuffer};
    use iox_query::exec::{
        DedicatedExecutor, Executor, ExecutorConfig, IOxSessionContext, PerQueryMemoryPoolConfig,
    };
    use iox_time::{MockProvider, Time, TimeProvider};
    use metric::Registry;
    use object_store::ObjectStore;
    use object_store::memory::InMemory;
    use observability_deps::tracing;
    use parquet_file::storage::{ParquetStorage, StorageId};
    use std::io::Write;
    use std::num::NonZeroUsize;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tempfile::NamedTempFile;
    use tokio::sync::{mpsc, oneshot, watch};
    use tokio::time::{Instant, advance, timeout};

    use crate::manager::ProcessingEngineError;

    #[test_log::test(tokio::test)]
    async fn test_trigger_lifecycle() -> influxdb3_write::write_buffer::Result<()> {
        let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
        let test_store = Arc::new(InMemory::new());
        let wal_config = WalConfig {
            gen1_duration: Gen1Duration::new_1m(),
            max_write_buffer_size: 100,
            flush_interval: Duration::from_millis(10),
            snapshot_size: 1,
            ..Default::default()
        };
        let (pem, file) = setup(start_time, test_store, wal_config).await;
        let file_name = file
            .path()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        // convert to Arc<WriteBuffer>
        let write_buffer: Arc<dyn WriteBuffer> = Arc::clone(&pem.write_buffer);

        // Create the DB by inserting a line.
        write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu,warehouse=us-east,room=01a,device=10001 reading=37\n",
                start_time,
                false,
                Precision::Nanosecond,
                false,
            )
            .await?;

        // Create an enabled trigger
        let file_name = pem
            .validate_plugin_filename(file_name.as_str())
            .await
            .unwrap();

        write_buffer
            .catalog()
            .create_processing_engine_trigger(
                "foo",
                "test_trigger",
                Arc::clone(&pem.node_id),
                file_name,
                &TriggerSpecificationDefinition::AllTablesWalWrite.string_rep(),
                TriggerSettings::default(),
                &None,
                false,
            )
            .await
            .unwrap();

        // Verify trigger is not disabled in schema
        let schema = write_buffer.catalog().db_schema("foo").unwrap();
        let trigger = schema
            .processing_engine_triggers
            .get_by_name("test_trigger")
            .unwrap();
        assert!(!trigger.disabled);

        // Disable the trigger
        write_buffer
            .catalog()
            .disable_processing_engine_trigger("foo", "test_trigger")
            .await
            .unwrap();

        // Verify trigger is disabled in schema
        let schema = write_buffer.catalog().db_schema("foo").unwrap();
        let trigger = schema
            .processing_engine_triggers
            .get_by_name("test_trigger")
            .unwrap();
        assert!(trigger.disabled);

        // Enable the trigger
        write_buffer
            .catalog()
            .enable_processing_engine_trigger("foo", "test_trigger")
            .await
            .unwrap();

        // Verify trigger is enabled and running
        let schema = write_buffer.catalog().db_schema("foo").unwrap();
        let trigger = schema
            .processing_engine_triggers
            .get_by_name("test_trigger")
            .unwrap();
        assert!(!trigger.disabled);
        Ok(())
    }

    #[tokio::test]
    async fn test_create_disabled_trigger() -> influxdb3_write::write_buffer::Result<()> {
        let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
        let test_store = Arc::new(InMemory::new());
        let wal_config = WalConfig {
            gen1_duration: Gen1Duration::new_1m(),
            max_write_buffer_size: 100,
            flush_interval: Duration::from_millis(10),
            snapshot_size: 1,
            ..Default::default()
        };
        let (pem, file) = setup(start_time, test_store, wal_config).await;
        let file_name = file
            .path()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        // Create the DB by inserting a line.
        pem.write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu,warehouse=us-east,room=01a,device=10001 reading=37\n",
                start_time,
                false,
                Precision::Nanosecond,
                false,
            )
            .await?;

        let file_name = pem.validate_plugin_filename(&file_name).await.unwrap();
        // Create a disabled trigger
        pem.catalog
            .create_processing_engine_trigger(
                "foo",
                "test_trigger",
                Arc::clone(&pem.node_id),
                file_name,
                &TriggerSpecificationDefinition::AllTablesWalWrite.string_rep(),
                TriggerSettings::default(),
                &None,
                true,
            )
            .await
            .unwrap();

        // Verify trigger is created but disabled
        let schema = pem.catalog.db_schema("foo").unwrap();
        let trigger = schema
            .processing_engine_triggers
            .get_by_name("test_trigger")
            .unwrap();
        assert!(trigger.disabled);

        // Verify trigger is not in active triggers list
        assert!(pem.catalog.active_triggers().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_enable_nonexistent_trigger() -> influxdb3_write::write_buffer::Result<()> {
        let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
        let test_store = Arc::new(InMemory::new());
        let wal_config = WalConfig {
            gen1_duration: Gen1Duration::new_1m(),
            max_write_buffer_size: 100,
            flush_interval: Duration::from_millis(10),
            snapshot_size: 1,
            ..Default::default()
        };
        let (pem, _file_name) = setup(start_time, test_store, wal_config).await;

        let write_buffer: Arc<dyn WriteBuffer> = Arc::clone(&pem.write_buffer);

        // Create the DB by inserting a line.
        write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu,warehouse=us-east,room=01a,device=10001 reading=37\n",
                start_time,
                false,
                Precision::Nanosecond,
                false,
            )
            .await?;

        let Err(CatalogError::NotFound) = pem
            .catalog
            .enable_processing_engine_trigger("foo", "nonexistent_trigger")
            .await
        else {
            panic!("should receive not found error for non existent trigger on enable");
        };

        Ok(())
    }

    #[test]
    fn bounded_shutdown_force_delete_unblocks_routing_and_catalog() {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap()
            .block_on(check_force_delete_while_cache_reclamation_waits());
    }

    async fn check_force_delete_while_cache_reclamation_waits() {
        let (pem, file) = setup(
            Time::from_timestamp_nanos(0),
            Arc::new(InMemory::new()),
            WalConfig::default(),
        )
        .await;
        pem.catalog.create_database("foo").await.unwrap();
        let key =
            create_test_trigger(&pem, &file, "request_name", "request:request_path", true).await;

        // A full event queue with a task that never acknowledges shutdown.
        let (request_tx, mut request_rx) = mpsc::channel(1);
        request_tx
            .try_send(Request {
                query_params: HashMap::new(),
                headers: HashMap::new(),
                body: Bytes::new(),
                response_tx: oneshot::channel().0,
            })
            .unwrap();
        let (dropped_tx, mut dropped_rx) = oneshot::channel::<()>();
        let (cancel_tx, mut cancel_rx) = watch::channel(false);
        pem.plugin_event_tx.write().await.insert(
            key,
            RunningTrigger {
                database_name: "foo".into(),
                trigger_name: "request_name".into(),
                route: TriggerRoute::Request("request_path".into(), request_tx),
                cache: PyCache::new_trigger_cache(Arc::clone(&pem.cache), cancel_tx.subscribe()),
                cancellation: cancel_tx,
                task: tokio::spawn(async move {
                    std::future::pending::<()>().await;
                    drop(dropped_tx);
                }),
            },
        );

        let captured = Arc::new(Mutex::new(Vec::new()));
        let writer = {
            let captured = Arc::clone(&captured);
            move || CapturedLog(Arc::clone(&captured))
        };
        let _subscriber = tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_writer(writer)
                .with_ansi(false)
                .finish(),
        );
        tracing::callsite::rebuild_interest_cache();

        tokio::time::pause();
        // Keep the reclamation job queued past the committed stop and its timeout.
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel::<()>();
        let blocker = tokio::task::spawn_blocking(move || {
            started_tx.send(()).unwrap();
            let _ = release_rx.blocking_recv();
        });
        started_rx.await.unwrap();

        let started = Instant::now();
        let pending_request =
            pem.request_trigger("request_path", HashMap::new(), HashMap::new(), Bytes::new());
        tokio::pin!(pending_request);
        assert!(futures_util::poll!(&mut pending_request).is_pending());

        let catalog = Arc::clone(&pem.catalog);
        let deletion = tokio::spawn(async move {
            catalog
                .delete_processing_engine_trigger("foo", "request_name", true)
                .await
        });
        timeout(
            Duration::from_secs(1),
            cancel_rx.wait_for(|cancelled| *cancelled),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(matches!(
            pending_request.await,
            Err(ProcessingEngineError::RequestTriggerNotFound)
        ));
        assert!(matches!(
            pem.request_trigger("request_path", HashMap::new(), HashMap::new(), Bytes::new())
                .await,
            Err(ProcessingEngineError::RequestTriggerNotFound)
        ));
        assert!(request_rx.try_recv().is_ok());
        assert!(request_rx.try_recv().is_err());
        assert!(
            pem.catalog
                .db_schema("foo")
                .unwrap()
                .processing_engine_triggers
                .get_by_name("request_name")
                .is_none()
        );

        timeout(
            Duration::from_secs(1),
            pem.catalog.create_database("unrelated"),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(pem.stop_trigger(key, None, "force_delete").await.is_none());

        let before_deadline = started + SHUTDOWN_BUDGET - Duration::from_secs(1);
        advance(before_deadline - Instant::now()).await;
        assert!(!deletion.is_finished());
        assert!(matches!(
            dropped_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        // A busy blocking pool prevents auto-advance, so include the timer's rounding tick.
        advance(Duration::from_secs(1) + Duration::from_millis(1)).await;
        deletion.await.unwrap().unwrap();
        assert!(dropped_rx.await.is_err());
        release_tx.send(()).unwrap();
        blocker.await.unwrap();
        assert!(
            (SHUTDOWN_BUDGET..=SHUTDOWN_BUDGET + Duration::from_millis(1))
                .contains(&started.elapsed())
        );

        let logs = captured.lock().unwrap();
        let logs = std::str::from_utf8(&logs).unwrap();
        let timeouts: Vec<_> = logs
            .lines()
            .filter(|line| line.contains("trigger shutdown timed out"))
            .collect();
        assert_eq!(timeouts.len(), 1);
        assert!(timeouts[0].contains("force_delete"));
        assert!(timeouts[0].contains("completion"));
    }

    #[tokio::test]
    async fn bounded_shutdown_preserves_replacement_and_accepts_closed_task() {
        let (pem, _) = setup(
            Time::from_timestamp_nanos(0),
            Arc::new(InMemory::new()),
            WalConfig::default(),
        )
        .await;
        tokio::time::pause();
        let started = Instant::now();
        let key = (influxdb3_id::DbId::new(1), influxdb3_id::TriggerId::new(1));
        let (cancel_tx, mut generation) = watch::channel(false);
        let (release_tx, release_rx) = oneshot::channel::<()>();
        pem.plugin_event_tx.write().await.insert(
            key,
            RunningTrigger {
                database_name: "foo".into(),
                trigger_name: "schedule".into(),
                route: TriggerRoute::Schedule,
                cache: PyCache::new_trigger_cache(Arc::clone(&pem.cache), cancel_tx.subscribe()),
                cancellation: cancel_tx,
                task: tokio::spawn(async {
                    let _ = release_rx.await;
                }),
            },
        );

        let old_completion = pem
            .stop_trigger(key, Some(&generation), "disable")
            .await
            .unwrap();
        generation.wait_for(|cancelled| *cancelled).await.unwrap();
        let (replacement_tx, replacement) = watch::channel(false);
        let closed_task = tokio::spawn(async {});
        let closed = closed_task.abort_handle();
        closed_task.abort();
        pem.plugin_event_tx.write().await.insert(
            key,
            RunningTrigger {
                database_name: "foo".into(),
                trigger_name: "schedule".into(),
                route: TriggerRoute::Schedule,
                cache: PyCache::new_trigger_cache(Arc::clone(&pem.cache), replacement.clone()),
                cancellation: replacement_tx,
                task: closed_task,
            },
        );

        assert!(
            pem.stop_trigger(key, Some(&generation), "self_disable")
                .await
                .is_none()
        );
        release_tx.send(()).unwrap();
        old_completion.await.unwrap();
        assert!(!*replacement.borrow());
        assert!(pem.plugin_event_tx.read().await.running.contains_key(&key));
        pem.stop_trigger(key, Some(&replacement), "disable")
            .await
            .unwrap()
            .await
            .unwrap();
        assert!(closed.is_finished());
        assert!(*replacement.borrow());
        assert_eq!(Instant::now(), started);
    }

    #[tokio::test]
    async fn bounded_shutdown_restart_routes_only_active_definitions() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (pem, file) = setup(
            Time::from_timestamp_nanos(0),
            Arc::clone(&store),
            WalConfig::default(),
        )
        .await;
        pem.catalog.create_database("foo").await.unwrap();
        let enabled = create_test_trigger(&pem, &file, "enabled", "request:enabled", false).await;
        let disabled =
            create_test_trigger(&pem, &file, "disabled", "request:disabled", false).await;
        let deleted = create_test_trigger(&pem, &file, "deleted", "request:deleted", false).await;
        tokio::time::pause();

        pem.catalog
            .disable_processing_engine_trigger("foo", "disabled")
            .await
            .unwrap();
        pem.catalog
            .delete_processing_engine_trigger("foo", "deleted", true)
            .await
            .unwrap();
        // Reload the persisted catalog and start a fresh runtime registry. No request runs Python.
        let catalog = Arc::new(
            Catalog::new(
                "test_host",
                store,
                Arc::clone(&pem.time_provider),
                Default::default(),
            )
            .await
            .unwrap(),
        );
        let restarted = ProcessingEngineManagerImpl::new(
            pem.environment_manager.clone(),
            catalog,
            pem.node_id(),
            Arc::clone(&pem.write_buffer),
            Arc::clone(&pem.query_executor),
            Arc::clone(&pem.time_provider),
            Arc::clone(&pem.sys_event_store),
        )
        .await;
        Arc::clone(&restarted).start_triggers().await.unwrap();
        let channels = restarted.plugin_event_tx.read().await;
        assert_eq!(channels.running.len(), 1);
        assert_eq!(channels.request_paths.get("enabled"), Some(&enabled));
        assert!(!channels.running.contains_key(&disabled));
        assert!(!channels.running.contains_key(&deleted));
    }

    async fn create_test_trigger(
        pem: &ProcessingEngineManagerImpl,
        file: &NamedTempFile,
        name: &str,
        spec: &str,
        disabled: bool,
    ) -> TriggerKey {
        let filename = file.path().file_name().unwrap().to_str().unwrap();
        pem.catalog
            .create_processing_engine_trigger(
                "foo",
                name,
                Arc::clone(&pem.node_id),
                pem.validate_plugin_filename(filename).await.unwrap(),
                spec,
                TriggerSettings::default(),
                &None,
                disabled,
            )
            .await
            .unwrap();
        let db = pem.catalog.db_schema("foo").unwrap();
        (
            db.id,
            db.processing_engine_triggers
                .get_by_name(name)
                .unwrap()
                .trigger_id,
        )
    }

    const BATCH_ERROR: &str = "error executing against batch";

    /// A WAL batch the engine can't turn into Python rows must be logged and dropped
    /// under `ErrorBehavior::Log`. It used to be retried forever against the same
    /// schema, logging every attempt (TAG-2772: about 20k lines a minute).
    #[tokio::test]
    async fn test_unconvertible_wal_batch_is_logged_once() {
        let settings = TriggerSettings {
            run_async: true,
            error_behavior: ErrorBehavior::Log,
        };
        let (pem, _file) = setup_with_trigger(
            "bad_batch",
            &TriggerSpecificationDefinition::AllTablesWalWrite.string_rep(),
            settings,
            &None,
        )
        .await;

        pem.notify(Arc::new(wal_contents_with_unknown_column(&pem)))
            .await;
        wait_for_engine_logs(&pem, "bad_batch", BATCH_ERROR, 1).await;
        // Room for a looping trigger to repeat the batch.
        tokio::time::sleep(Duration::from_millis(300)).await;

        assert_eq!(engine_log_count(&pem, "bad_batch", BATCH_ERROR), 1);
    }

    /// The engine's error line must not carry trigger arguments: they hold webhook
    /// secrets, API keys and tokens, and the line ends up in the log store.
    #[tokio::test]
    async fn test_batch_error_log_omits_trigger_arguments() {
        let captured = Arc::new(Mutex::new(Vec::new()));
        let writer = {
            let captured = Arc::clone(&captured);
            move || CapturedLog(Arc::clone(&captured))
        };
        let subscriber = tracing_subscriber::fmt()
            .with_writer(writer)
            .with_ansi(false)
            .finish();
        // The test runtime is single-threaded, so the plugin task logs on this thread.
        let _guard = tracing::subscriber::set_default(subscriber);

        let args = Some(hashbrown::HashMap::from([(
            "twilio_token".to_string(),
            "not-a-real-token-6d1f".to_string(),
        )]));
        let settings = TriggerSettings {
            run_async: true,
            error_behavior: ErrorBehavior::Log,
        };
        let (pem, _file) = setup_with_trigger(
            "secret_args",
            &TriggerSpecificationDefinition::AllTablesWalWrite.string_rep(),
            settings,
            &args,
        )
        .await;

        // Other tests hit the same log callsites with no subscriber; make sure the
        // cached interest includes this one before the batch is sent.
        tracing::callsite::rebuild_interest_cache();
        pem.notify(Arc::new(wal_contents_with_unknown_column(&pem)))
            .await;
        wait_for_engine_logs(&pem, "secret_args", BATCH_ERROR, 1).await;

        let logs = String::from_utf8(captured.lock().unwrap().clone()).unwrap();
        assert!(
            logs.contains("error running against batch"),
            "the batch error was not captured: {logs}"
        );
        assert!(
            logs.contains("secret_args"),
            "the batch error doesn't name its trigger: {logs}"
        );
        assert!(
            !logs.contains("not-a-real-token-6d1f"),
            "a trigger argument value reached the log: {logs}"
        );
    }

    /// A sync trigger (`run_async: false`, the CLI default) runs its batches in its own
    /// loop, so the old endless retry also stopped it from ever reading the next batch.
    #[tokio::test]
    async fn test_sync_trigger_runs_the_next_batch_after_one_it_cannot_convert() {
        let settings = TriggerSettings {
            run_async: false,
            error_behavior: ErrorBehavior::Log,
        };
        let (pem, _file) = setup_with_trigger(
            "sync_bad_batch",
            &TriggerSpecificationDefinition::AllTablesWalWrite.string_rep(),
            settings,
            &None,
        )
        .await;

        pem.notify(Arc::new(wal_contents_with_unknown_column(&pem)))
            .await;
        pem.notify(Arc::new(wal_contents_with_reading(&pem))).await;

        // The test plugin logs "done" for each batch it receives.
        wait_for_engine_logs(&pem, "sync_bad_batch", "done", 1).await;
        assert_eq!(engine_log_count(&pem, "sync_bad_batch", BATCH_ERROR), 1);
    }

    /// A forced delete removes the trigger from the catalog before the engine is told,
    /// and the engine used to find the trigger to stop through the catalog. The running
    /// WAL trigger then kept receiving every write, and nothing could disable it.
    #[tokio::test]
    async fn test_force_deleted_wal_trigger_stops_receiving_writes() {
        let (pem, _file) = setup_with_trigger(
            "doomed_wal",
            &TriggerSpecificationDefinition::AllTablesWalWrite.string_rep(),
            TriggerSettings::default(),
            &None,
        )
        .await;
        assert!(wal_trigger_registered(&pem, "doomed_wal").await);

        pem.catalog
            .delete_processing_engine_trigger("foo", "doomed_wal", true)
            .await
            .unwrap();

        assert!(
            !wal_trigger_registered(&pem, "doomed_wal").await,
            "a force-deleted WAL trigger still receives writes"
        );
    }

    /// The same for a scheduled trigger: after a forced delete it must not keep running.
    #[tokio::test]
    async fn test_force_deleted_schedule_trigger_stops_running() {
        let (pem, _file) = setup_with_trigger(
            "doomed_schedule",
            "every:1h",
            TriggerSettings::default(),
            &None,
        )
        .await;
        assert!(schedule_trigger_registered(&pem, "doomed_schedule").await);

        pem.catalog
            .delete_processing_engine_trigger("foo", "doomed_schedule", true)
            .await
            .unwrap();

        assert!(
            !schedule_trigger_registered(&pem, "doomed_schedule").await,
            "a force-deleted scheduled trigger is still running"
        );
    }

    /// Creates database `foo` with table `cpu`, then an enabled trigger on it.
    async fn setup_with_trigger(
        trigger_name: &str,
        trigger_spec: &str,
        settings: TriggerSettings,
        args: &Option<hashbrown::HashMap<String, String>>,
    ) -> (Arc<ProcessingEngineManagerImpl>, NamedTempFile) {
        let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
        let wal_config = WalConfig {
            gen1_duration: Gen1Duration::new_1m(),
            max_write_buffer_size: 100,
            flush_interval: Duration::from_millis(10),
            snapshot_size: 1,
            ..Default::default()
        };
        let (pem, file) = setup(start_time, Arc::new(InMemory::new()), wal_config).await;
        pem.write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu,warehouse=us-east,room=01a,device=10001 reading=37\n",
                start_time,
                false,
                Precision::Nanosecond,
                false,
            )
            .await
            .unwrap();
        let file_name = file.path().file_name().unwrap().to_str().unwrap();
        let file_name = pem.validate_plugin_filename(file_name).await.unwrap();
        pem.catalog
            .create_processing_engine_trigger(
                "foo",
                trigger_name,
                Arc::clone(&pem.node_id),
                file_name,
                trigger_spec,
                settings,
                args,
                false,
            )
            .await
            .unwrap();
        (pem, file)
    }

    /// One write to `foo.cpu` whose only field has a column id the table doesn't have,
    /// so the engine can't convert it (`field not found`).
    fn wal_contents_with_unknown_column(pem: &ProcessingEngineManagerImpl) -> WalContents {
        cpu_write(pem, Field::new(ColumnId::new(9_999), FieldData::Float(1.0)))
    }

    /// One write to `foo.cpu` with a `reading` value, which the engine converts.
    fn wal_contents_with_reading(pem: &ProcessingEngineManagerImpl) -> WalContents {
        let cpu = pem
            .catalog
            .db_schema("foo")
            .unwrap()
            .table_definition("cpu")
            .unwrap();
        let reading = cpu.column_name_to_id("reading").unwrap();
        cpu_write(pem, Field::new(reading, FieldData::Float(37.0)))
    }

    /// One WAL write to `foo.cpu` holding a single row with `field`.
    fn cpu_write(pem: &ProcessingEngineManagerImpl, field: Field) -> WalContents {
        let db = pem.catalog.db_schema("foo").unwrap();
        let table_id = db.table_name_to_id("cpu").unwrap();
        let row = Row {
            time: 0,
            fields: vec![field],
        };
        let mut table_chunks = SerdeVecMap::new();
        table_chunks.insert(
            table_id,
            TableChunks {
                min_time: 0,
                max_time: 0,
                chunk_time_to_chunk: hashbrown::HashMap::from([(
                    0,
                    TableChunk { rows: vec![row] },
                )]),
            },
        );
        WalContents {
            persist_timestamp_ms: 0,
            min_timestamp_ns: 0,
            max_timestamp_ns: 0,
            wal_file_number: WalFileSequenceNumber::new(1),
            ops: vec![WalOp::Write(WriteBatch {
                catalog_sequence: 0,
                database_id: db.id,
                database_name: Arc::clone(&db.name),
                table_chunks,
                min_time_ns: 0,
                max_time_ns: 0,
            })],
            snapshot: None,
        }
    }

    /// Lines in `system.processing_engine_logs` for this trigger that start with `prefix`.
    fn engine_log_count(
        pem: &ProcessingEngineManagerImpl,
        trigger_name: &str,
        prefix: &str,
    ) -> usize {
        let Some(batch) = pem.sys_event_store.as_record_batch::<ProcessingEngineLog>() else {
            return 0;
        };
        let batch = batch.unwrap();
        let column = |name: &str| {
            batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .clone()
        };
        let (names, texts) = (column("trigger_name"), column("log_text"));
        (0..batch.num_rows())
            .filter(|&i| names.value(i) == trigger_name && texts.value(i).starts_with(prefix))
            .count()
    }

    async fn wait_for_engine_logs(
        pem: &ProcessingEngineManagerImpl,
        trigger_name: &str,
        prefix: &str,
        at_least: usize,
    ) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while engine_log_count(pem, trigger_name, prefix) < at_least {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("no '{prefix}' log for {trigger_name} within 10s"));
    }

    async fn wal_trigger_registered(pem: &ProcessingEngineManagerImpl, trigger_name: &str) -> bool {
        pem.plugin_event_tx
            .read()
            .await
            .running
            .values()
            .any(|runtime| {
                runtime.database_name.as_ref() == "foo"
                    && runtime.trigger_name.as_ref() == trigger_name
                    && matches!(runtime.route, TriggerRoute::Wal(_))
            })
    }

    async fn schedule_trigger_registered(
        pem: &ProcessingEngineManagerImpl,
        trigger_name: &str,
    ) -> bool {
        pem.plugin_event_tx
            .read()
            .await
            .running
            .values()
            .any(|runtime| {
                runtime.database_name.as_ref() == "foo"
                    && runtime.trigger_name.as_ref() == trigger_name
                    && matches!(runtime.route, TriggerRoute::Schedule)
            })
    }

    struct CapturedLog(Arc<Mutex<Vec<u8>>>);

    impl Write for CapturedLog {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    async fn setup(
        start: Time,
        object_store: Arc<dyn ObjectStore>,
        wal_config: WalConfig,
    ) -> (Arc<ProcessingEngineManagerImpl>, NamedTempFile) {
        let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(start));
        let metric_registry = Arc::new(Registry::new());
        let persister = Arc::new(Persister::new(
            Arc::clone(&object_store),
            "test_host",
            Arc::clone(&time_provider) as _,
        ));
        let catalog = Arc::new(
            Catalog::new(
                "test_host",
                Arc::clone(&object_store),
                Arc::clone(&time_provider),
                Default::default(),
            )
            .await
            .unwrap(),
        );
        let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog) as _)
            .await
            .unwrap();
        let distinct_cache = DistinctCacheProvider::new_from_catalog(
            Arc::clone(&time_provider),
            Arc::clone(&catalog),
        )
        .await
        .unwrap();
        let shutdown = ShutdownManager::new_testing();
        let wbuf = WriteBufferImpl::new(WriteBufferImplArgs {
            persister,
            catalog: Arc::clone(&catalog),
            last_cache,
            distinct_cache,
            time_provider: Arc::clone(&time_provider),
            executor: make_exec(),
            wal_config,
            parquet_cache: None,
            metric_registry: Arc::clone(&metric_registry),
            snapshotted_wal_files_to_keep: 10,
            query_file_limit: None,
            shutdown: shutdown.register(),
            n_snapshots_to_load_on_start: N_SNAPSHOTS_TO_LOAD_ON_START,
            wal_replay_concurrency_limit: Some(1),
        })
        .await
        .unwrap();
        let ctx = IOxSessionContext::with_testing();
        let runtime_env = ctx.inner().runtime_env();
        register_iox_object_store(runtime_env, "influxdb3", Arc::clone(&object_store));

        let qe = Arc::new(UnimplementedQueryExecutor);

        let mut file = NamedTempFile::new().unwrap();
        let code = r#"
def process_writes(influxdb3_local, table_batches, args=None):
    influxdb3_local.info("done")
"#;
        writeln!(file, "{code}").unwrap();
        let environment_manager = ProcessingEngineEnvironmentManager {
            plugin_dir: Some(file.path().parent().unwrap().to_path_buf()),
            virtual_env_location: None,
            package_manager: Arc::new(DisabledManager),
        };

        let sys_event_store = Arc::new(SysEventStore::new(Arc::clone(&time_provider)));

        (
            ProcessingEngineManagerImpl::new(
                environment_manager,
                catalog,
                "test_node",
                wbuf,
                qe,
                time_provider,
                sys_event_store,
            )
            .await,
            file,
        )
    }

    pub(crate) fn make_exec() -> Arc<Executor> {
        let metrics = Arc::new(metric::Registry::default());
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let parquet_store = ParquetStorage::new(
            Arc::clone(&object_store),
            StorageId::from("test_exec_storage"),
        );
        Arc::new(Executor::new_with_config_and_executor(
            ExecutorConfig {
                target_query_partitions: NonZeroUsize::new(1).unwrap(),
                object_stores: [&parquet_store]
                    .into_iter()
                    .map(|store| (store.id(), Arc::clone(store.object_store())))
                    .collect(),
                metric_registry: Arc::clone(&metrics),
                // Default to 1gb
                mem_pool_size: 1024 * 1024 * 1024, // 1024 (b/kb) * 1024 (kb/mb) * 1024 (mb/gb)
                heap_memory_limit: None,
                per_query_mem_pool_config: PerQueryMemoryPoolConfig::Disabled,
            },
            DedicatedExecutor::new_testing(),
        ))
    }
}
