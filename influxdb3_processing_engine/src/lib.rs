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
use influxdb3_internal_api::query_executor::QueryExecutor;
use influxdb3_py_api::system_py::CacheStore;
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
use tokio::sync::{RwLock, mpsc, oneshot};

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

#[derive(Debug, Default)]
struct PluginChannels {
    /// Map of database to wal trigger name to handler
    wal_triggers: HashMap<String, HashMap<String, mpsc::Sender<WalEvent>>>,
    /// Map of database to schedule trigger name to handler
    schedule_triggers: HashMap<String, HashMap<String, mpsc::Sender<ScheduleEvent>>>,
    /// Map of request path to the request trigger handler
    request_triggers: HashMap<String, mpsc::Sender<RequestEvent>>,
}

const PLUGIN_EVENT_BUFFER_SIZE: usize = 60;

impl PluginChannels {
    // returns Ok(Some(receiver)) if there was a sender to the named trigger.
    async fn send_shutdown(
        &self,
        db: String,
        trigger: String,
        plugin_type: PluginType,
    ) -> Result<Option<Receiver<()>>, ProcessingEngineError> {
        match plugin_type {
            PluginType::WalRows => {
                if let Some(trigger_map) = self.wal_triggers.get(&db) {
                    if let Some(sender) = trigger_map.get(&trigger) {
                        // create a one shot to wait for the shutdown to complete
                        let (tx, rx) = oneshot::channel();
                        if sender.send(WalEvent::Shutdown(tx)).await.is_err() {
                            return Err(ProcessingEngineError::TriggerShutdownError {
                                database: db,
                                trigger_name: trigger,
                            });
                        }
                        return Ok(Some(rx));
                    }
                }
            }
            PluginType::Schedule => {
                if let Some(trigger_map) = self.schedule_triggers.get(&db) {
                    if let Some(sender) = trigger_map.get(&trigger) {
                        // create a one shot to wait for the shutdown to complete
                        let (tx, rx) = oneshot::channel();
                        if sender.send(ScheduleEvent::Shutdown(tx)).await.is_err() {
                            return Err(ProcessingEngineError::TriggerShutdownError {
                                database: db,
                                trigger_name: trigger,
                            });
                        }
                        return Ok(Some(rx));
                    }
                }
            }
            PluginType::Request => {
                if let Some(sender) = self.request_triggers.get(&trigger) {
                    // create a one shot to wait for the shutdown to complete
                    let (tx, rx) = oneshot::channel();
                    if sender.send(RequestEvent::Shutdown(tx)).await.is_err() {
                        return Err(ProcessingEngineError::TriggerShutdownError {
                            database: db,
                            trigger_name: trigger,
                        });
                    }
                    return Ok(Some(rx));
                }
            }
        }

        Ok(None)
    }

    fn remove_trigger(&mut self, db: String, trigger: String, plugin_type: PluginType) {
        match plugin_type {
            PluginType::WalRows => {
                if let Some(trigger_map) = self.wal_triggers.get_mut(&db) {
                    trigger_map.remove(&trigger);
                }
            }
            PluginType::Schedule => {
                if let Some(trigger_map) = self.schedule_triggers.get_mut(&db) {
                    trigger_map.remove(&trigger);
                }
            }
            PluginType::Request => {
                self.request_triggers.remove(&trigger);
            }
        }
    }

    /// The type of a running WAL or scheduled trigger, found by name. Used when the
    /// catalog no longer has the trigger but its plugin may still be running.
    fn running_plugin_type(&self, db: &str, trigger: &str) -> Option<PluginType> {
        if self
            .wal_triggers
            .get(db)
            .is_some_and(|triggers| triggers.contains_key(trigger))
        {
            Some(PluginType::WalRows)
        } else if self
            .schedule_triggers
            .get(db)
            .is_some_and(|triggers| triggers.contains_key(trigger))
        {
            Some(PluginType::Schedule)
        } else {
            None
        }
    }

    fn add_wal_trigger(&mut self, db: String, trigger: String) -> mpsc::Receiver<WalEvent> {
        let (tx, rx) = mpsc::channel(PLUGIN_EVENT_BUFFER_SIZE);
        self.wal_triggers.entry(db).or_default().insert(trigger, tx);
        rx
    }

    fn add_schedule_trigger(
        &mut self,
        db: String,
        trigger: String,
    ) -> mpsc::Receiver<ScheduleEvent> {
        let (tx, rx) = mpsc::channel(PLUGIN_EVENT_BUFFER_SIZE);
        self.schedule_triggers
            .entry(db)
            .or_default()
            .insert(trigger, tx);
        rx
    }

    fn add_request_trigger(&mut self, path: String) -> mpsc::Receiver<RequestEvent> {
        let (tx, rx) = mpsc::channel(PLUGIN_EVENT_BUFFER_SIZE);
        self.request_triggers.insert(path, tx);
        rx
    }

    async fn send_wal_contents(&self, wal_contents: Arc<WalContents>) {
        for (db, trigger_map) in &self.wal_triggers {
            for (trigger, sender) in trigger_map {
                if let Err(e) = sender
                    .send(WalEvent::WriteWalContents(Arc::clone(&wal_contents)))
                    .await
                {
                    warn!(%e, %db, ?trigger, "error sending wal contents to plugin");
                }
            }
        }
    }

    async fn send_request(
        &self,
        trigger_path: &str,
        request: Request,
    ) -> Result<(), ProcessingEngineError> {
        let event = RequestEvent::Request(request);
        if let Some(sender) = self.request_triggers.get(trigger_path) {
            if sender.send(event).await.is_err() {
                return Err(ProcessingEngineError::RequestTriggerNotFound);
            }
        } else {
            return Err(ProcessingEngineError::RequestTriggerNotFound);
        }

        Ok(())
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
    // TODO(trevor): should this be id-based and not use names?
    async fn run_trigger(
        self: Arc<Self>,
        db_name: &str,
        trigger_name: &str,
    ) -> Result<(), ProcessingEngineError> {
        debug!(db_name, trigger_name, "starting trigger");

        {
            let db_schema = self
                .catalog
                .db_schema(db_name)
                .ok_or_else(|| ProcessingEngineError::DatabaseNotFound(db_name.to_string()))?;
            let trigger = db_schema
                .processing_engine_triggers
                .get_by_name(trigger_name)
                .clone()
                .ok_or_else(|| CatalogError::ProcessingEngineTriggerNotFound {
                    database_name: db_name.to_string(),
                    trigger_name: trigger_name.to_string(),
                })?;

            if trigger.node_id != self.node_id {
                error!(
                    "Not running trigger {}, as it is configured for node id {}. Multi-node not supported in core, so this shouldn't happen.",
                    trigger_name, trigger.node_id
                );
                return Ok(());
            }

            let plugin_context = PluginContext {
                write_buffer: Arc::clone(&self.write_buffer),
                query_executor: Arc::clone(&self.query_executor),
                sys_event_store: Arc::clone(&self.sys_event_store),
                manager: Arc::clone(&self),
            };
            let plugin_code = Arc::new(self.read_plugin_code(&trigger.plugin_filename).await?);
            match trigger.trigger.plugin_type() {
                PluginType::WalRows => {
                    let rec = self
                        .plugin_event_tx
                        .write()
                        .await
                        .add_wal_trigger(db_name.to_string(), trigger_name.to_string());

                    plugins::run_wal_contents_plugin(
                        db_name.to_string(),
                        plugin_code,
                        trigger,
                        plugin_context,
                        rec,
                    )
                }
                PluginType::Schedule => {
                    let rec = self
                        .plugin_event_tx
                        .write()
                        .await
                        .add_schedule_trigger(db_name.to_string(), trigger_name.to_string());

                    plugins::run_schedule_plugin(
                        db_name.to_string(),
                        plugin_code,
                        trigger,
                        Arc::clone(&self.time_provider),
                        plugin_context,
                        rec,
                    )?
                }
                PluginType::Request => {
                    let TriggerSpecificationDefinition::RequestPath { path } = &trigger.trigger
                    else {
                        unreachable!()
                    };
                    let rec = self
                        .plugin_event_tx
                        .write()
                        .await
                        .add_request_trigger(path.to_string());

                    plugins::run_request_plugin(
                        db_name.to_string(),
                        plugin_code,
                        trigger,
                        plugin_context,
                        rec,
                    )
                }
            }
        }

        Ok(())
    }

    // TODO(trevor): should this be id-based and not use names?
    async fn stop_trigger(
        &self,
        db_name: &str,
        trigger_name: &str,
    ) -> Result<(), ProcessingEngineError> {
        let db_schema = self
            .catalog
            .db_schema(db_name)
            .ok_or_else(|| ProcessingEngineError::DatabaseNotFound(db_name.to_string()))?;
        // A forced delete reaches the catalog before this runs, so a trigger that is
        // still running may already be gone from it. Find that one by name, or it
        // would keep running with no way to stop it.
        let plugin_type = match db_schema
            .processing_engine_triggers
            .get_by_name(trigger_name)
        {
            Some(trigger) => trigger.trigger.plugin_type(),
            None => self
                .plugin_event_tx
                .read()
                .await
                .running_plugin_type(db_name, trigger_name)
                .ok_or_else(|| CatalogError::ProcessingEngineTriggerNotFound {
                    database_name: db_name.to_string(),
                    trigger_name: trigger_name.to_string(),
                })?,
        };

        let Some(shutdown_rx) = self
            .plugin_event_tx
            .write()
            .await
            .send_shutdown(db_name.to_string(), trigger_name.to_string(), plugin_type)
            .await?
        else {
            return Ok(());
        };

        if shutdown_rx.await.is_err() {
            warn!(
                "shutdown trigger receiver dropped, may have received multiple shutdown requests"
            );
        } else {
            self.plugin_event_tx.write().await.remove_trigger(
                db_name.to_string(),
                trigger_name.to_string(),
                plugin_type,
            );
        }
        self.cache
            .lock()
            .drop_trigger_cache(db_name.to_string(), trigger_name.to_string());

        Ok(())
    }

    pub async fn start_triggers(self: Arc<Self>) -> Result<(), ProcessingEngineError> {
        let triggers = self.catalog.active_triggers();
        for (db_name, trigger_name) in triggers {
            Arc::clone(&self)
                .run_trigger(&db_name, &trigger_name)
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

        self.plugin_event_tx
            .write()
            .await
            .send_request(trigger_path, request)
            .await?;

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
        let plugin_channels = self.plugin_event_tx.read().await;
        plugin_channels.send_wal_contents(write).await;
    }

    async fn notify_and_snapshot(
        &self,
        write: Arc<WalContents>,
        snapshot_details: SnapshotDetails,
    ) -> Receiver<SnapshotDetails> {
        let plugin_channels = self.plugin_event_tx.read().await;
        plugin_channels.send_wal_contents(write).await;

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
    Shutdown(oneshot::Sender<()>),
}

#[allow(dead_code)]
pub(crate) enum ScheduleEvent {
    Shutdown(oneshot::Sender<()>),
}

#[allow(dead_code)]
pub(crate) enum RequestEvent {
    Request(Request),
    Shutdown(oneshot::Sender<()>),
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
        while let Some(catalog_update) = subscription.recv().await {
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
                            disabled,
                            ..
                        }) => {
                            if !disabled {
                                if let Err(error) = processing_engine_manager
                                    .run_trigger(database_name, trigger_name)
                                    .await
                                {
                                    error!(?error, "failed to run the created trigger");
                                }
                            }
                        }
                        DatabaseCatalogOp::EnableTrigger(TriggerIdentifier {
                            db_name,
                            trigger_name,
                            ..
                        }) => {
                            if let Err(error) = processing_engine_manager
                                .run_trigger(db_name, trigger_name)
                                .await
                            {
                                error!(?error, "failed to run the trigger");
                            }
                        }
                        DatabaseCatalogOp::DeleteTrigger(DeleteTriggerLog {
                            trigger_name,
                            force: true,
                            ..
                        }) => {
                            if let Err(error) = processing_engine_manager
                                .stop_trigger(&batch.database_name, trigger_name)
                                .await
                            {
                                error!(?error, "failed to disable the trigger");
                            }
                        }
                        DatabaseCatalogOp::DisableTrigger(TriggerIdentifier {
                            db_name,
                            trigger_name,
                            ..
                        }) => {
                            if let Err(error) = processing_engine_manager
                                .stop_trigger(db_name, trigger_name)
                                .await
                            {
                                error!(?error, "failed to disable the trigger");
                            }
                        }
                        // NOTE(trevor/catalog-refactor): it is not clear that any other operation needs to be
                        // handled, based on the existing code, but we could potentially
                        // handle database deletion, trigger creation/deletion/enable here
                        _ => (),
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use crate::ProcessingEngineManagerImpl;
    use crate::environment::DisabledManager;
    use crate::plugins::ProcessingEngineEnvironmentManager;
    use arrow_array::{Array, StringArray};
    use data_types::NamespaceName;
    use datafusion_util::config::register_iox_object_store;
    use influxdb3_cache::distinct_cache::DistinctCacheProvider;
    use influxdb3_cache::last_cache::LastCacheProvider;
    use influxdb3_catalog::CatalogError;
    use influxdb3_catalog::catalog::Catalog;
    use influxdb3_catalog::log::{ErrorBehavior, TriggerSettings, TriggerSpecificationDefinition};
    use influxdb3_id::{ColumnId, SerdeVecMap};
    use influxdb3_internal_api::query_executor::UnimplementedQueryExecutor;
    use influxdb3_py_api::logging::ProcessingEngineLog;
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
        let db = pem.catalog.db_schema("foo").unwrap();
        let table_id = db.table_name_to_id("cpu").unwrap();
        let row = Row {
            time: 0,
            fields: vec![Field::new(ColumnId::new(9_999), FieldData::Float(1.0))],
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
            .wal_triggers
            .get("foo")
            .is_some_and(|triggers| triggers.contains_key(trigger_name))
    }

    async fn schedule_trigger_registered(
        pem: &ProcessingEngineManagerImpl,
        trigger_name: &str,
    ) -> bool {
        pem.plugin_event_tx
            .read()
            .await
            .schedule_triggers
            .get("foo")
            .is_some_and(|triggers| triggers.contains_key(trigger_name))
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
