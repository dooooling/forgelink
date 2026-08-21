//! Collector 控制链路装配（§81/§90/§98）——`control` feature 门控。
//!
//! 只读构建（`--no-default-features --features collector`）**不编译本
//! 模块**：无 ControlEngine、无控制路由、无凭据加载（§98 验收要求，
//! 由 `cargo check -p collector --no-default-features --features collector`
//! 与产物符号检查共同保证）。
//!
//! # 装配顺序（§100 扩展，两阶段）
//!
//! - 阶段 A（任何采集组件启动前，[`ControlStatic::load`]）：加载 §90.2
//!   凭据文件（缺失/非法/权限过宽即启动失败，fail-closed）、打开幂等
//!   Journal（JSONL，§80.1）、构造并校验控制策略——全部是同步文件 I/O
//!   与纯校验，失败直接返回错误，无需回收已启动组件；
//! - 阶段 B（设备注册后、REST 启动前，[`assemble`]）：由注册后的设备
//!   全集构建 DeviceCatalog、构造 DeviceControlExecutor（路由到共享
//!   Driver 会话，读写同锁 §82）与 ControlEngine、包装 rest-api 的
//!   [`ControlGateway`]——纯内存构造，不再失败。
//!
//! # 停机（§93/§104）
//!
//! [`ControlStack::shutdown`] 在 REST 关闭（停机第 0 步，不再受理新控制
//! 请求）之后、采集停止之前调用：在途控制动作优先结算/取消（设备在采集
//! 链路停止前进入确定状态），宽限期内未完成则强制中止并按
//! Indeterminate/Cancelled 结算——收据与 Journal 全部落定，不残留 Running。
//!
//! # 审计（§90）
//!
//! [`TracingAuditSink`] 把每条审计事件写入结构化日志（与 §6 日志基础
//! 设施同一脱敏管道；参数摘要由引擎侧脱敏——字符串/字节只记长度与哈希
//! 前缀）。`record` 实现只做日志宏调用（非阻塞），满足 AuditSink 的
//! "尽量非阻塞"契约；后续可替换为文件/远程审计后端。

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use control_engine::{
    AuditEvent, AuditSink, ControlEngine, ControlEngineConfig, ControlPolicy, DeviceInfo,
    FileJournal, MemoryDeviceCatalog, StaticTokenAuthorizer,
};
use device_manager::{DeviceControlExecutor, DeviceManager};
use rest_api::{ControlGateway, EngineControlAdapter};
use tracing::{info, warn};

use crate::config::CollectorConfig;
use crate::error::CollectorError;

/// 缺省幂等 Journal 文件名（落在数据目录——与 WAL 同目录，§103）。
const DEFAULT_JOURNAL_FILE: &str = "control-journal.jsonl";

/// 审计输出（§90）：结构化日志落地。
///
/// 每条审计事件以结构化字段写 tracing 日志；`record` 内无 I/O 等待，
/// 引擎侧另有 `audit_timeout_ms` 有界超时兜底（§90 慢审计不阻塞控制）。
struct TracingAuditSink;

#[async_trait::async_trait]
impl AuditSink for TracingAuditSink {
    async fn record(&self, event: AuditEvent) {
        // 字段级输出（§6 结构化日志约定）；参数摘要已由引擎侧脱敏，
        // 此处原样透传，不追加任何原始报文内容。
        info!(
            component = "control-audit",
            user = %event.user,
            source = %event.source,
            namespace = %event.namespace,
            device_id = %event.device_id,
            request_id = %event.request_id,
            operation = ?event.operation,
            target = %event.target,
            parameters = ?event.parameters,
            risk_level = ?event.risk_level,
            status = ?event.status,
            error_code = event.error_code.as_deref().unwrap_or(""),
            protocol_code = ?event.protocol_code,
            duration_ms = event.duration_ms,
            occurred_at_ns = event.occurred_at_ns,
            "控制审计事件"
        );
    }
}

/// 阶段 A 产物：启动路径的同步装配结果（§90.2 fail-closed）。
pub(crate) struct ControlStatic {
    /// Bearer Token 认证器（REST 认证与引擎授权同源，§90.2）。
    authorizer: Arc<StaticTokenAuthorizer>,
    /// 幂等 Journal（JSONL 落盘，跨重启恢复未结算为 Indeterminate）。
    journal: Arc<FileJournal>,
    /// 控制策略（已通过装配期校验，§86/§80.1）。
    policy: Arc<ControlPolicy>,
    /// 控制命名空间（幂等键三元组之一）。
    namespace: String,
    /// REST 提交的默认控制超时（毫秒）。
    default_timeout_ms: u64,
    /// 引擎停机宽限。
    shutdown_grace: Duration,
}

impl ControlStatic {
    /// 阶段 A：加载凭据、打开 Journal、构造并校验策略。
    ///
    /// 在任何采集组件启动之前调用：全部操作为同步文件 I/O 与纯校验，
    /// 失败直接返回 [`CollectorError::Control`]（fail-closed），无需回收
    /// 已启动组件。
    pub(crate) fn load(config: &CollectorConfig) -> Result<Self, CollectorError> {
        // 配置校验（§100）已保证 control 构建必填本段；此处兜底防御。
        let options = config
            .control
            .as_ref()
            .ok_or_else(|| CollectorError::Control("配置缺少 control 段".to_owned()))?;

        // §90.2：凭据文件装配期同步加载；缺失/非法/权限过宽一律启动失败
        // （fail-closed），不得静默降级为无认证控制。
        let authorizer =
            StaticTokenAuthorizer::from_file(&options.credentials_file).map_err(|e| {
                CollectorError::Control(format!(
                    "加载控制凭据文件 {} 失败: {e}",
                    options.credentials_file.display()
                ))
            })?;

        // Journal 路径：显式配置优先；缺省与 WAL 同一数据目录（§103 数据
        // 目录约定），幂等记录与采集数据同生命周期。打开失败（父目录缺失/
        // 文件损坏 Corrupt）即启动失败——损坏行静默跳过会导致重启后同一
        // 请求重复执行（§80.1）。
        let journal_path = match &options.journal_path {
            Some(path) => path.clone(),
            None => default_journal_path(&config.buffer.db_path),
        };
        let journal = FileJournal::open(&journal_path, crate::now_ns()).map_err(|e| {
            CollectorError::Control(format!(
                "打开控制幂等 Journal {} 失败: {e}",
                journal_path.display()
            ))
        })?;

        // 策略：安全相关默认值（角色门槛/优先级/幂等保留期 ≥24h）不开放
        // 配置，仅覆盖运维子集；装配期校验 fail-fast（ControlEngine::new
        // 对非法策略 panic，此处先 validate 保证不可达）。
        let policy = Arc::new(ControlPolicy {
            queue_capacity: options.queue_capacity,
            audit_timeout_ms: options.audit_timeout_ms,
            ..ControlPolicy::default()
        });
        if let Err(reason) = policy.validate() {
            return Err(CollectorError::Control(format!("控制策略非法: {reason}")));
        }

        Ok(Self {
            authorizer: Arc::new(authorizer),
            journal: Arc::new(journal),
            policy,
            namespace: options.namespace.clone(),
            default_timeout_ms: options.timeout_ms,
            shutdown_grace: Duration::from_millis(options.shutdown_grace_ms),
        })
    }
}

/// 缺省 Journal 路径：`<buffer.db_path 父目录>/control-journal.jsonl`；
/// db_path 无父目录（纯文件名）时落在工作目录。
fn default_journal_path(buffer_db_path: &Path) -> PathBuf {
    match buffer_db_path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.join(DEFAULT_JOURNAL_FILE),
        _ => PathBuf::from(DEFAULT_JOURNAL_FILE),
    }
}

/// 阶段 B 产物：REST 控制网关（挂载控制路由）与引擎停机句柄。
pub(crate) struct ControlAttachment {
    /// 传给 `RestApiServer::spawn_with_control` 的控制网关。
    pub(crate) gateway: ControlGateway,
    /// 引擎停机句柄（关闭责任属于运行时）。
    pub(crate) stack: ControlStack,
}

/// 控制引擎运行句柄。
///
/// `ControlEngine` 是 `Arc` 语义的轻量克隆；适配器与停机句柄各持一份，
/// 停机经由 [`ControlStack::shutdown`] 统一执行（不依赖适配器存活）。
pub(crate) struct ControlStack {
    engine: ControlEngine,
    shutdown_grace: Duration,
}

impl ControlStack {
    /// 引擎有序停机（§81/§93）：停止受理 → 每设备队列并发排空（grace
    /// 有界）→ 超时强制中止并结算遗留条目。收据与 Journal 全部落定，
    /// 不残留 Running。
    pub(crate) async fn shutdown(&self) {
        self.engine.shutdown(self.shutdown_grace).await;
        info!(component = "collector", "控制引擎已停机");
    }
}

/// 阶段 B：由注册后的设备全集装配控制引擎与 REST 网关。
///
/// 纯内存构造，不失败。设备目录含**禁用**设备——引擎对禁用设备以
/// `DEVICE_DISABLED` 明确拒绝，而非"未知设备"（§84 可诊断性）。
pub(crate) fn assemble(statics: ControlStatic, manager: &Arc<DeviceManager>) -> ControlAttachment {
    let mut catalog = MemoryDeviceCatalog::new();
    for device_id in manager.device_ids() {
        // device_ids 与注册表同源，get 理论上必命中；防御式跳过而非
        // panic（生产路径禁 panic）。
        let Some(instance) = manager.get(device_id) else {
            warn!(
                component = "collector",
                device_id = %device_id,
                "设备目录构建：设备已注册但实例缺失，跳过"
            );
            continue;
        };
        catalog.insert(
            device_id.clone(),
            DeviceInfo::new(instance.device.enabled, Arc::clone(&instance.profile)),
        );
    }

    // 执行器：按 device_id 路由到 DeviceManager 的共享 Driver 会话，
    // 写入/命令与 Poll Engine 读取共用同一把会话锁（§82）。
    let executor = Arc::new(DeviceControlExecutor::new(Arc::clone(manager)));

    let engine = ControlEngine::new(ControlEngineConfig {
        catalog: Arc::new(catalog),
        authorizer: statics.authorizer.clone(),
        journal: statics.journal,
        executor,
        audit: Arc::new(TracingAuditSink),
        policy: statics.policy,
    });

    // REST 适配层：namespace/默认超时来自 control 配置（§32.2 服务端
    // 提供信封字段）；网关与引擎共用同一 authorizer，保证 REST 认证出的
    // subject/角色与引擎授权一致（§90.2）。查询角色门槛取策略默认
    // （Operator，§86）。
    let adapter = EngineControlAdapter::new(
        engine.clone(),
        statics.namespace,
        statics.default_timeout_ms,
    );
    let gateway = ControlGateway::new(Arc::new(adapter), statics.authorizer);

    info!(
        component = "collector",
        "控制链路已装配（ControlEngine + REST 控制路由）"
    );
    ControlAttachment {
        gateway,
        stack: ControlStack {
            engine,
            shutdown_grace: statics.shutdown_grace,
        },
    }
}
