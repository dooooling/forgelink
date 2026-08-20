//! Collector 运行时组装（§93/§100）。
//!
//! 启动顺序：Load Config → Load Driver → Load Profile → Register Device
//! → Start Pipeline / Buffer / MQTT → Polling → Observation → Upload。
//!
//! 链路（只读，§98/§106）：
//!
//! ```text
//! Poll Engine ── PollEvent ──> DeviceManager 映射（Profile+Domain）── Observations
//!   ──> Data Pipeline（按设备聚合组包）──> Local Buffer/WAL（落盘）
//!   ──> 发送循环：next ──> MQTT publish_batch ──> PUBACK ──> ack（唯一删除路径）
//!   失败（Closed/Disconnected/CollisionOverwritten）──> requeue（保留补传）
//! ```
//!
//! 停机编排（有序 + 有限排空）：
//!
//! ```text
//! 信号 ──> PollScheduler.shutdown（停止采集）
//!      ──> pump join（剩余事件映射入管道）
//!      ──> Pipeline.shutdown（组包排空到输出端）
//!      ──> forward 排空（输出端收完 + WAL 能发的发完，期限内）
//!      ──> MqttClient.shutdown（DISCONNECT，未确认以 Closed 结算，WAL 保留）
//!      ──> LocalBuffer.shutdown（未确认记录保留，下次启动补传）
//! ```

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use device_manager::{DeviceManager, NativeDriverFactory};
use driver_loader::NativePlugin;
use driver_sdk::DriverManifest;
use observation_model::Device;
use poll_engine::PollScheduler;
use tokio::sync::{mpsc, watch};
use tracing::{error, info, warn};

use crate::config::CollectorConfig;
use crate::error::CollectorError;
use crate::health::CollectorHealth;
use crate::rest::CollectorApiState;
use crate::tasks::{HealthState, run_forward, run_heartbeat, run_pump};

/// 心跳日志周期（§104 长期稳定性观测）。
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(60);

/// Collector 运行时：持有全部组件与工作任务句柄。
pub struct CollectorRuntime {
    site_id: String,
    session_id: String,
    health: Arc<HealthState>,
    devices: Vec<(String, bool, usize, usize)>,
    scheduler: PollScheduler,
    pipeline: Arc<data_pipeline::Pipeline>,
    buffer: Arc<local_buffer::LocalBuffer>,
    mqtt: Option<Arc<mqtt_client::MqttClient>>,
    /// REST v1 只读接口（§31.5；配置未启用时为 `None`）。`Arc` 允许
    /// 外部监督方持有句柄副本订阅异常退出通知（`exit_notified`）。
    rest: Option<Arc<rest_api::RestApiServer>>,
    pump: tokio::task::JoinHandle<()>,
    // 内部 Result 上报永久性落盘错误等（评审 P1）。
    forward: tokio::task::JoinHandle<Result<(), CollectorError>>,
    // 发送循环结果是否已被消费（评审 P1：JoinHandle 输出只能取一次，
    // `run_until_shutdown` 的 select 分支消费后 `shutdown` 不得再次
    // join，否则 panic "JoinHandle polled after completion"）。
    forward_done: bool,
    // 落盘失败批次收尾队列（评审 P1：push 永久失败/暂停期间容量超限
    // 的批次不静默丢弃——停机流程在 MQTT 结算后限时重试落盘）。
    lost_rx: mpsc::Receiver<data_pipeline::ObservationBatch>,
    heartbeat: tokio::task::JoinHandle<()>,
    stopping: watch::Sender<bool>,
    config: CollectorConfig,
}

impl CollectorRuntime {
    /// 加载配置并按 §100 顺序组装全部组件，随后立即开始采集。
    ///
    /// 失败语义：任何组件启动失败（Profile 缺失 / Driver ABI 不符 /
    /// 设备绑定失败 / 管道或缓冲配置非法）都显式返回错误，不静默降级。
    pub async fn start(config: CollectorConfig) -> Result<Self, CollectorError> {
        config.validate()?;
        let session_id = config.effective_session_id();
        let started_at_ns = crate::now_ns();
        info!(
            component = "collector",
            site_id = %config.site_id,
            session_id = %session_id,
            "Collector 启动"
        );

        // 1) Load Profile（§38：Profile 不写死在主程序）。
        let mut registry = profile_engine::ProfileRegistry::new();
        let loaded = registry
            .load_dir(&config.profiles_dir)
            .map_err(|e| CollectorError::Profiles(e.to_string()))?;
        info!(
            component = "collector",
            profiles_dir = %config.profiles_dir.display(),
            profiles = loaded,
            "Device Profile 已加载"
        );

        // 2) Load Driver（§19/§20：Native Plugin + Manifest）。
        let manifest = DriverManifest {
            id: config.driver.manifest.id.clone(),
            name: config.driver.manifest.name.clone(),
            version: config.driver.manifest.version.clone(),
            entry: driver_sdk::abi::ENTRY_SYMBOL.to_owned(),
            abi: driver_sdk::manifest::AbiVersion {
                major: config.driver.manifest.abi.major,
                minor: config.driver.manifest.abi.minor,
            },
            platforms: vec![],
        };
        let plugin = Arc::new(
            NativePlugin::load(&config.driver.plugin, manifest)
                .map_err(|e| CollectorError::Driver(Box::new(e)))?,
        );
        info!(
            component = "collector",
            plugin = %config.driver.plugin.display(),
            driver_id = %config.driver.manifest.id,
            "Driver 已加载"
        );
        let mut factory = NativeDriverFactory::new();
        factory
            .add_plugin(plugin)
            .map_err(|e| CollectorError::Driver(Box::new(e)))?;

        // 3) 构造设备（domain 缺省取 Profile 决定，§100 device.yaml），
        //    随后注册绑定 Driver/Profile 并生成读取项（§37）。
        let mut devices: Vec<Device> = Vec::with_capacity(config.devices.len());
        for spec in &config.devices {
            let domain = match &spec.domain {
                Some(d) => d.clone(),
                None => registry
                    .get(&spec.profile)
                    .ok_or_else(|| {
                        CollectorError::Profiles(format!(
                            "设备 {} 引用的 Profile {} 未注册",
                            spec.id, spec.profile
                        ))
                    })?
                    .domain
                    .clone(),
            };
            devices.push(Device {
                id: spec.id.clone(),
                name: spec.name.clone().unwrap_or_else(|| spec.id.clone()),
                domain,
                driver_id: spec.driver.clone(),
                profile_id: spec.profile.clone(),
                connection: observation_model::DeviceConnection {
                    config: spec.connection.clone(),
                },
                enabled: spec.enabled,
                labels: spec.labels.clone(),
            });
        }
        let mut manager =
            DeviceManager::new(registry, Box::new(factory), config.poll.default_interval_ms)?;
        for device in &devices {
            manager.register_device(device.clone())?;
            info!(
                component = "collector",
                device_id = %device.id,
                driver_id = %device.driver_id,
                profile_id = %device.profile_id,
                "设备已注册"
            );
        }

        // 4) 数据管道 / 本地缓冲 / MQTT（§31.2/§103/§31）。
        let pipeline_cfg = config
            .pipeline
            .to_pipeline_config(&config.site_id, &session_id)?;
        let (out_tx, out_rx) = mpsc::channel(pipeline_cfg.max_batch_size);
        let pipeline = data_pipeline::Pipeline::spawn(pipeline_cfg, out_tx)?;
        let pipeline_arc = Arc::new(pipeline);

        let buffer_cfg = config.buffer.to_buffer_config()?;
        if let Some(parent) = buffer_cfg.db_path.parent()
            && !parent.as_os_str().is_empty()
            && !parent.exists()
        {
            warn!(
                component = "collector",
                db_dir = %parent.display(),
                "缓冲数据库父目录不存在（重启恢复依赖目录持久化，请提前创建）"
            );
        }
        let buffer = Arc::new(local_buffer::LocalBuffer::open(buffer_cfg).await?);

        let mqtt_cfg = config.northbound.mqtt.to_client_config(&config.site_id)?;
        let mqtt = mqtt_client::MqttClient::spawn(mqtt_cfg)?;
        let mqtt_arc = Arc::new(mqtt);

        // 5) 在线状态（§31.1：retained status envelope，每设备；断线后
        //    mqtt-client 按设备全集重建重发周期，失败不阻塞启动）。
        for device_id in manager.device_ids() {
            let instance = manager.get(device_id).expect("已注册");
            if instance.device.enabled {
                if let Err(e) = mqtt_arc.publish_online(&config.site_id, device_id).await {
                    warn!(
                        component = "collector",
                        device_id,
                        error = %e,
                        "在线状态发布失败（重连后自动重建重发）"
                    );
                }
            } else {
                // 禁用设备：发布 retained 离线并注销在线跟踪，清除
                // Broker 中之前保留的在线状态——设备从启用变为禁用后
                // 不得长期显示在线（评审 P2）。
                if let Err(e) = mqtt_arc.publish_offline(&config.site_id, device_id).await {
                    warn!(
                        component = "collector",
                        device_id,
                        error = %e,
                        "离线状态发布失败（重连后自动重建重发）"
                    );
                }
            }
        }

        // 6) Poll Scheduler：按读取项分组启动轮询任务（§22/§34.3）。
        let poll_cfg = config.poll.to_poll_config();
        let (events_tx, events_rx) = mpsc::channel(256);
        let mut scheduler = PollScheduler::new();
        let mut device_meta: Vec<(String, bool, usize, usize)> = Vec::new();
        for device_id in manager.device_ids() {
            let instance = manager.get(device_id).expect("已注册");
            let targets = instance.poll_targets();
            device_meta.push((
                device_id.clone(),
                instance.device.enabled,
                targets.iter().map(|t| t.items.len()).sum(),
                targets.len(),
            ));
            for target in &targets {
                scheduler.spawn(
                    target.clone(),
                    instance.driver.clone(),
                    poll_cfg.clone(),
                    events_tx.clone(),
                )?;
            }
            if targets.is_empty() {
                warn!(
                    component = "collector",
                    device_id, "设备没有可采集的读取项（Profile 无可读属性或设备被禁用）"
                );
            }
        }

        // 7) 工作任务：pump（事件→映射→管道）、forward（管道→缓冲→MQTT）、
        //    heartbeat（健康状态日志，§104）。
        let (stopping_tx, stopping_rx) = watch::channel(false);
        // 落盘失败批次收尾队列（评审 P1）：有界——容量超限期间的
        // 暂存上限，满时 forward 明确结算丢弃，不无限堆积内存。
        let (lost_tx, lost_rx) = mpsc::channel(128);
        let health = Arc::new(HealthState::default());
        health.started_at_ns.store(started_at_ns, Ordering::Relaxed);

        // 8) REST v1 只读管理接口（§31.5/§104）：默认禁用，显式配置
        //    `rest.listen` 才启动（§90.1 只监听 loopback）。绑定失败
        //    fail-fast（用户显式配置了端口而不可用，不静默降级），且
        //    必须先回收已启动组件——轮询任务/MQTT 客户端/管道/缓冲，
        //    不得遗留后台任务与阻塞 Driver 调用（评审 P2）。
        //    设备元数据在 `manager` 移入 `manager_arc` 之前提取（注册后
        //    静态不变，REST 快照的静态部分）。
        let rest = if let Some(listen) = &config.rest.listen {
            let listen = listen
                .parse::<std::net::SocketAddr>()
                .map_err(|e| CollectorError::Rest(format!("监听地址 {listen:?} 非法: {e}")))?;
            let meta = crate::rest::extract_device_meta(&manager);
            let state = Arc::new(CollectorApiState::new(
                Arc::clone(&health),
                config.site_id.clone(),
                session_id.clone(),
                meta,
            ));
            let server = match rest_api::RestApiServer::spawn(
                state,
                rest_api::RestConfig {
                    listen,
                    max_concurrency: config.rest.max_concurrency,
                },
            )
            .await
            {
                Ok(server) => server,
                Err(e) => {
                    // 启动失败收尾（评审 P2）：轮询任务可能正持有阻塞
                    // 的 Driver 调用，必须先取消再释放各组件；清理失败
                    // 只告警，原始 REST 错误优先返回。
                    // 等待必须有界（评审 P1）：Native Driver 卡死时不得
                    // 让启动无限挂起，超时强制 abort 轮询任务。
                    let error = CollectorError::Rest(format!("监听 {listen} 失败: {e}"));
                    scheduler
                        .shutdown_with_timeout(Duration::from_secs(5))
                        .await;
                    if let Ok(mqtt) = Arc::try_unwrap(mqtt_arc)
                        && let Err(e) = mqtt.shutdown().await
                    {
                        warn!(
                            component = "collector",
                            error = %e,
                            "启动失败清理：MQTT 结算失败"
                        );
                    }
                    if let Ok(pipeline) = Arc::try_unwrap(pipeline_arc)
                        && let Err(e) = pipeline.shutdown().await
                    {
                        warn!(
                            component = "collector",
                            error = %e,
                            "启动失败清理：管道关闭失败"
                        );
                    }
                    if let Err(e) = buffer.shutdown().await {
                        warn!(
                            component = "collector",
                            error = %e,
                            "启动失败清理：缓冲关闭失败"
                        );
                    }
                    return Err(error);
                }
            };
            info!(
                component = "collector",
                addr = %server.addr,
                "REST v1 只读管理接口已启用"
            );
            Some(Arc::new(server))
        } else {
            None
        };

        let manager_arc = Arc::new(manager);
        let pump = tokio::spawn(run_pump(
            events_rx,
            Arc::clone(&manager_arc),
            Arc::clone(&pipeline_arc),
            session_id.clone(),
            Arc::clone(&health),
        ));
        let forward = tokio::spawn(run_forward(
            out_rx,
            Arc::clone(&buffer),
            Arc::clone(&mqtt_arc),
            stopping_rx.clone(),
            Arc::clone(&health),
            Duration::from_millis(config.forward_poll_ms.max(50)),
            Duration::from_secs(config.buffer.drain_timeout_secs),
            lost_tx,
        ));
        let heartbeat = tokio::spawn(run_heartbeat(
            stopping_rx.clone(),
            Arc::clone(&health),
            HEARTBEAT_INTERVAL,
        ));

        let runtime = Self {
            site_id: config.site_id.clone(),
            session_id: session_id.clone(),
            health: Arc::clone(&health),
            devices: device_meta,
            scheduler,
            pipeline: pipeline_arc,
            buffer,
            mqtt: Some(mqtt_arc),
            rest,
            pump,
            forward,
            forward_done: false,
            lost_rx,
            heartbeat,
            stopping: stopping_tx,
            config,
        };
        runtime.log_health("启动完成").await;
        Ok(runtime)
    }

    /// 站点标识。
    pub fn site_id(&self) -> &str {
        &self.site_id
    }

    /// 采集会话标识。
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// 健康状态快照（§104：设备/缓冲/发布三面；REST 阶段暴露 HTTP 端点）。
    pub fn health(&self) -> CollectorHealth {
        let mut h = self.health.snapshot(&self.devices);
        h.site_id = self.site_id.clone();
        h.session_id = self.session_id.clone();
        h.buffer.db_path = self.config.buffer.db_path.display().to_string();
        h
    }

    /// REST 接口实际监听地址（未启用或服务已退出时为 `None`；评审 P2：
    /// `serve` 任务异常退出后地址失效，避免向已死端口发起请求）。
    pub fn rest_addr(&self) -> Option<std::net::SocketAddr> {
        let rest = self.rest.as_ref()?;
        rest.is_alive().then_some(rest.addr)
    }

    /// REST 服务是否存活（未启用时为 `false`）。
    pub fn rest_alive(&self) -> bool {
        self.rest.as_ref().is_some_and(|s| s.is_alive())
    }

    /// REST 服务句柄副本（未启用时为 `None`）。外部监督方（如未来的
    /// manager/运维工具）可据此订阅异常退出通知（`exit_notified`）或
    /// 探测存活；**关闭责任始终属于运行时**——`shutdown` 为可共享调用
    /// （`&self`），停机时无条件发送停止信号（评审 P1：外部持有副本
    /// 不得阻止 REST 随 Collector 一并关闭）。
    pub fn rest_server(&self) -> Option<Arc<rest_api::RestApiServer>> {
        self.rest.clone()
    }

    async fn log_health(&self, stage: &str) {
        let h = self.health();
        if h.has_device_errors() {
            warn!(component = "collector", stage, health = %h, "Collector 健康状态（存在设备异常）");
        } else {
            info!(component = "collector", stage, health = %h, "Collector 健康状态");
        }
    }

    /// 并行监督运行并在任一终止条件满足时执行有序停机（§104）。
    ///
    /// **必须紧跟 `start` 调用**：forward 异常退出与 REST 服务异常
    /// 退出从启动后立即被监视，不得等到外部停机信号才感知——否则
    /// REST 已不可用/发送循环已终止时采集仍静默运行（评审 P1）。
    /// 系统信号（SIGINT/SIGTERM）在本函数内与 forward/REST 监视并行
    /// 监听。
    ///
    /// 终止条件：
    /// - 系统信号：`ctrl_c`（SIGINT）；SIGTERM 由 main 挂接任务置位
    ///   `signal`（§104 Service Restart 语义，Windows 不可用时跳过）。
    /// - 发送循环异常退出（永久性落盘错误等，内部 Result 上报）。
    /// - REST serve 任务错误退出（API 不可用但采集继续属于静默故障，
    ///   评审 P2）。正常停机路径（`shutdown` 先发 stop 信号）不触发。
    pub async fn run_until_shutdown(
        mut self,
        mut signal: watch::Receiver<bool>,
    ) -> Result<(), CollectorError> {
        // REST 异常退出监视（评审 P2）：serve 任务错误退出时通知本
        // 运行时，触发错误上报与有序停机——API 不可用但采集继续属于
        // 静默故障。正常停机路径（shutdown 先发 stop 信号）不触发。
        let mut rest_exit = self.rest.as_ref().map(|s| s.exit_notified());
        // 启动竞态（评审 P1）：`watch::Receiver` 只报告**订阅后**的新
        // 变化——若 REST 任务在本函数订阅前已异常退出，通知已置位但
        // `changed()` 永远不会触发。此处先检查当前存活状态与已置位
        // 值，任一表明服务已不可用即立即按异常退出处理，不得因错过
        // 通知而继续采集。
        if let (Some(rest), Some(rx)) = (self.rest.as_ref(), rest_exit.as_ref()) {
            if !rest.is_alive() || *rx.borrow() {
                let msg = "REST 服务在监督启动前已不可用，采集停止（API 不可用）".to_owned();
                error!(component = "collector", "{msg}");
                self.stopping.send(true).ok();
                return Err(match self.shutdown().await {
                    Ok(()) => CollectorError::Task(msg),
                    Err(e) => CollectorError::Task(format!("{msg}；有序停机失败: {e}")),
                });
            }
        }
        tokio::select! {
            // 系统信号：SIGINT（Ctrl+C）直接监听；SIGTERM 由 main 挂接
            // 任务置位 `signal`（Windows 不支持 SIGTERM，main 仅在
            // UNIX 挂接）。停机信号通道关闭也按停机处理。
            _ = tokio::signal::ctrl_c() => {
                info!(component = "collector", "收到 SIGINT（Ctrl+C）");
            }
            r = signal.changed() => {
                if r.is_err() {
                    warn!(component = "collector", "停机信号通道关闭，按停机处理");
                }
            }
            r = &mut self.forward => {
                // 发送循环结束（非停机路径）：永久性落盘错误等以内部
                // Result 上报，或任务异常终止。
                let msg = match r {
                    Ok(Ok(())) => "发送循环提前退出".to_owned(),
                    Ok(Err(e)) => format!("发送循环错误退出: {e}"),
                    Err(e) => format!("发送循环任务异常退出: {e:?}"),
                };
                error!(component = "collector", "{msg}");
                self.stopping.send(true).ok();
                // 发送循环结果已在此取出（JoinHandle 输出只能取一次，
                // 评审 P1）：置位后 shutdown 跳过再次 join。仍执行完整
                // 有序停机（清理轮询/管道/MQTT/缓冲后台任务并收尾，
                // 否则遗留任务、尚未进入 WAL 的管道数据可能丢失）。
                // 原始错误优先，停机自身的失败并入错误返回。
                self.forward_done = true;
                return Err(match self.shutdown().await {
                    Ok(()) => CollectorError::Task(msg),
                    Err(e) => CollectorError::Task(format!("{msg}；有序停机失败: {e}")),
                });
            }
            r = async {
                match rest_exit.as_mut() {
                    Some(rx) => match rx.changed().await {
                        Ok(()) => *rx.borrow(),
                        // 通道关闭（全部发送端已释放，服务已退出）：按
                        // 异常处理，不得因忽略通道关闭而继续采集（评审
                        // P1：订阅竞态与通道关闭情况都不得被漏检）。
                        Err(_) => true,
                    },
                    None => std::future::pending::<bool>().await,
                }
            }, if self.rest.is_some() => {
                if r {
                    // REST serve 任务错误退出：API 已不可用。错误上报
                    // （日志 + 返回错误）并触发有序停机，不静默运行
                    // （评审 P2）。
                    let msg = "REST 服务异常退出，采集停止（API 不可用）".to_owned();
                    error!(component = "collector", "{msg}");
                    self.stopping.send(true).ok();
                    return Err(match self.shutdown().await {
                        Ok(()) => CollectorError::Task(msg),
                        Err(e) => CollectorError::Task(format!("{msg}；有序停机失败: {e}")),
                    });
                }
            }
        }
        info!(component = "collector", "收到停机信号，开始有序停机");
        self.shutdown().await
    }

    /// 有序停机（§104/§31.3：有限排空 + 未确认记录保留，重启补传）。
    pub async fn shutdown(mut self) -> Result<(), CollectorError> {
        self.stopping.send(true).ok();
        // 停机全程的健康快照：字段级取出，避免后续部分 move 的借用冲突。
        let final_health = self.health();

        // 0) REST 接口最先停止（独立任务、有界排空）：拒绝新连接后
        //    API 不可达，采集/WAL/MQTT 不受影响（§31.5 运行时接入）。
        //    停机是可共享调用（`shutdown(&self)`）：即使外部监督方持有
        //    `Arc` 副本，此处也无条件发送停止信号并等待排空——关闭
        //    责任属于运行时，不得转交给外部副本（评审 P1：外部持有
        //    句柄时 REST 不得在采集/MQTT/WAL 关闭后继续监听并响应）。
        if let Some(rest) = self.rest.take() {
            rest.shutdown().await;
        }

        // 1) 停止采集：轮询任务退出后事件通道关闭，pump 收 None 结束。
        self.scheduler.shutdown().await;
        let pump_result = tokio::time::timeout(Duration::from_secs(5), &mut self.pump)
            .await
            .map_err(|_| CollectorError::ShutdownTimeout { stage: "pump" })?;
        if let Err(e) = pump_result {
            warn!(component = "collector", error = %e, "pump 任务异常结束");
        }

        // 2) 管道排空：剩余 Observation 组包输出到 forward 的输出端。
        //    pump 已退出，Arc 引用可安全收回消费式 shutdown。
        let pipeline = Arc::try_unwrap(self.pipeline)
            .map_err(|_| CollectorError::Task("pipeline 引用未释放".to_owned()))?;
        let drained = pipeline.shutdown().await?;
        info!(
            component = "collector",
            batches = drained.batches_emitted,
            observations = drained.observations_emitted,
            dropped = drained.observations_dropped,
            "管道排空完成"
        );

        // 3) 发送循环有限排空：输出端收完 + WAL 能发的发完（期限内；
        //    超时或发布失败的记录保留，下次启动补传）。join 超时随
        //    配置的排空期限并留编排余量（评审 P2：固定 30s 会把更长
        //    的排空配置截断成 ShutdownTimeout）。结果已被
        //    `run_until_shutdown` 消费时（forward_done）跳过再次 join
        //    ——JoinHandle 输出只能取一次（评审 P1）。
        let forward_result = if self.forward_done {
            None
        } else {
            let forward_drain = Duration::from_secs(self.config.buffer.drain_timeout_secs);
            Some(
                tokio::time::timeout(forward_drain + Duration::from_secs(30), &mut self.forward)
                    .await
                    .map_err(|_| CollectorError::ShutdownTimeout { stage: "forward" })?,
            )
        };
        match forward_result {
            None => {}
            Some(Ok(Ok(()))) => {}
            Some(Ok(Err(e))) => {
                warn!(component = "collector", error = %e, "发送循环错误退出（记录保留补传）")
            }
            Some(Err(e)) => warn!(component = "collector", error = %e, "发送循环任务异常结束"),
        }

        // 4) MQTT 结算：DISCONNECT（不触发 LWT）+ 未确认发布以 Closed
        //    结算——WAL 记录不删除（保留补传，§31.3）。forward 已退出，
        //    引用可收回。结算失败**不短路**（评审 P1：失败批次的收尾
        //    落盘与缓冲关闭必须执行，否则未落盘批次永久丢失）——错误
        //    记录并合并到最终返回。
        let mut settle_error: Option<CollectorError> = None;
        if let Some(mqtt) = self.mqtt.take() {
            let mqtt = Arc::try_unwrap(mqtt)
                .map_err(|_| CollectorError::Task("mqtt 引用未释放".to_owned()))?;
            if let Err(e) = mqtt.shutdown().await {
                error!(
                    component = "collector",
                    error = %e,
                    "MQTT 停机结算失败（继续收尾：失败批次落盘与缓冲关闭仍执行）"
                );
                settle_error = Some(CollectorError::Mqtt(e));
            }
        }

        // 4.5) 落盘失败批次收尾（评审 P1）：push 永久失败或容量等待
        //     超时的批次进入收尾队列，不静默丢弃。这里限时重试落盘
        //     ——成功则随下次启动按序补传（replayed）；失败明确结算
        //     （告警丢弃）。限时必要：Backpressure 容量不足时 push
        //     等待 ACK 释放，但发布已停止，等待无望。逐批小限时 +
        //     总预算：容量持续满时不得逐批拖长停机。
        let lost_budget = Duration::from_secs(5);
        let lost_deadline = tokio::time::Instant::now() + lost_budget;
        while let Ok(batch) = self.lost_rx.try_recv() {
            let remaining = lost_deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                error!(
                    component = "collector",
                    message_id = %batch.message_id,
                    "落盘失败批次收尾预算耗尽，明确结算丢弃"
                );
                continue;
            }
            let attempt = remaining.min(Duration::from_millis(200));
            match tokio::time::timeout(attempt, self.buffer.push(batch.clone())).await {
                Ok(Ok(())) => {
                    info!(
                        component = "collector",
                        message_id = %batch.message_id,
                        "落盘失败批次收尾重试成功（下次启动补传）"
                    );
                }
                Ok(Err(e)) => {
                    error!(
                        component = "collector",
                        message_id = %batch.message_id,
                        error = %e,
                        "落盘失败批次无法入 WAL，明确结算丢弃"
                    );
                }
                Err(_) => {
                    error!(
                        component = "collector",
                        message_id = %batch.message_id,
                        "落盘失败批次等待容量超时，明确结算丢弃"
                    );
                }
            }
        }

        // 5) 缓冲关闭：未确认记录保留（§103 停机语义）。
        self.buffer.shutdown().await?;

        // 6) 心跳任务结束。
        self.heartbeat.abort();
        let _ = (&mut self.heartbeat).await;

        info!(
            component = "collector",
            site_id = %self.site_id,
            session_id = %self.session_id,
            health = %final_health,
            "Collector 有序停机完成"
        );
        if let Some(e) = settle_error {
            return Err(e);
        }
        Ok(())
    }
}
