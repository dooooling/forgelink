//! Control Engine 集成测试（§81-§90 验收）。
//!
//! 覆盖：写/命令统一入口、Driver 前拒绝（校验/授权/前置条件）、每设备串行、
//! 幂等 Duplicate/Conflict 与重启恢复（Indeterminate 不重放）、队列满、
//! 超时、取消、优先级、审计、停机语义。

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use driver_sdk::{
    DriverCommand, DriverErrorInfo, DriverWriteItem, RawCommandResult, RawWriteResult,
};
use observation_model::{
    CommandParameter, CommandRequest, CommandRiskLevel, ControlOperation, ControlPayloadResult,
    ControlRequest, ControlStatus, DeviceId, PropertyWriteItem, PropertyWriteRequest, Value,
};
use tokio::sync::watch;

use crate::audit::{AuditOperation, MemoryAuditSink};
use crate::catalog::{DeviceCatalog, MemoryDeviceCatalog, tests::profile_for_test};
use crate::engine::{ControlEngine, ControlEngineConfig, StatusQuery, SubmitContext, SubmitError};
use crate::executor::{ControlExecutor, ExecuteOutcome, WriteOutcome};
use crate::journal::{
    ControlJournal, FileJournal, IdempotencyKey, InMemoryJournal, JournalDecision, JournalError,
};
use crate::policy::{CommandPriority, ControlPolicy, Priority};
use crate::precondition::{PreconditionChecker, PreconditionError};
use crate::queue::now_ns;
use crate::role::{Authorizer, MemoryAuthorizer, Role};

// ---- 测试替身 ---------------------------------------------------------------

/// 可编程执行器：默认立即成功；可切换 失败/不确定/阻塞（release 放行）。
struct MockExecutor {
    calls: Mutex<Vec<String>>,
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
    fail: RwLock<Option<DriverErrorInfo>>,
    indeterminate: RwLock<bool>,
    partial_fail: AtomicBool,
    panic_once: AtomicBool,
    release: watch::Sender<bool>,
    _keep_rx: watch::Receiver<bool>,
}

impl MockExecutor {
    fn new() -> Arc<Self> {
        let (release, rx) = watch::channel(false);
        Arc::new(Self {
            calls: Mutex::new(Vec::new()),
            in_flight: AtomicUsize::new(0),
            max_in_flight: AtomicUsize::new(0),
            fail: RwLock::new(None),
            indeterminate: RwLock::new(false),
            partial_fail: AtomicBool::new(false),
            panic_once: AtomicBool::new(false),
            release,
            _keep_rx: rx,
        })
    }

    /// 下一次调用时 panic（四审 P2：worker panic 恢复测试用）。
    fn set_panic_once(&self) {
        self.panic_once.store(true, Ordering::SeqCst);
    }

    fn set_fail(&self, info: DriverErrorInfo) {
        *self.fail.write().expect("fail 锁被毒化") = Some(info);
    }

    fn set_indeterminate(&self) {
        *self.indeterminate.write().expect("indeterminate 锁被毒化") = true;
    }

    /// 批量写入部分失败（id 为奇数的项 success=false，P2-12）。
    fn set_partial_fail(&self) {
        self.partial_fail.store(true, Ordering::SeqCst);
    }

    fn release(&self) {
        let _ = self.release.send(true);
    }

    fn call_count(&self) -> usize {
        self.calls.lock().expect("calls 锁被毒化").len()
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().expect("calls 锁被毒化").clone()
    }

    fn max_concurrent(&self) -> usize {
        self.max_in_flight.load(Ordering::SeqCst)
    }

    /// 记录调用并（除非已 release）阻塞等待放行。
    async fn enter(&self, label: String) {
        self.calls.lock().expect("calls 锁被毒化").push(label);
        if self.panic_once.swap(false, Ordering::SeqCst) {
            panic!("模拟执行器 panic");
        }
        let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_in_flight.fetch_max(now, Ordering::SeqCst);
        let mut rx = self.release.subscribe();
        while !*rx.borrow() {
            let _ = rx.changed().await;
        }
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
    }

    fn outcome_error(&self) -> Option<DriverErrorInfo> {
        self.fail.read().expect("fail 锁被毒化").clone()
    }

    fn outcome_indeterminate(&self) -> bool {
        *self.indeterminate.read().expect("indeterminate 锁被毒化")
    }
}

#[async_trait]
impl ControlExecutor for MockExecutor {
    async fn write(&self, _device_id: &DeviceId, items: &[DriverWriteItem]) -> WriteOutcome {
        let label = format!(
            "write:{}",
            items
                .iter()
                .map(|i| format!("{}={i:?}", i.address))
                .collect::<Vec<_>>()
                .join(",")
        );
        self.enter(label).await;
        if let Some(info) = self.outcome_error() {
            return WriteOutcome::Failed(info);
        }
        if self.outcome_indeterminate() {
            return WriteOutcome::Indeterminate(DriverErrorInfo {
                code: "device_unreachable".to_owned(),
                message: "写入后无法确认".to_owned(),
                protocol_code: None,
                retryable: true,
            });
        }
        if self.partial_fail.load(Ordering::SeqCst) {
            return WriteOutcome::Succeeded(
                items
                    .iter()
                    .map(|i| RawWriteResult {
                        item_id: i.id,
                        success: i.id % 2 == 0,
                        protocol_code: if i.id % 2 == 0 { Some(0) } else { Some(0x86) },
                        error: (i.id % 2 == 1).then(|| DriverErrorInfo {
                            code: "modbus_exception".to_owned(),
                            message: "slave 拒绝该项".to_owned(),
                            protocol_code: Some(0x86),
                            retryable: false,
                        }),
                    })
                    .collect(),
            );
        }
        WriteOutcome::Succeeded(
            items
                .iter()
                .map(|i| RawWriteResult {
                    item_id: i.id,
                    success: true,
                    protocol_code: Some(0),
                    error: None,
                })
                .collect(),
        )
    }

    async fn execute(&self, _device_id: &DeviceId, command: &DriverCommand) -> ExecuteOutcome {
        let label = format!("cmd:{}:{command:?}", command.command_id);
        self.enter(label).await;
        if let Some(info) = self.outcome_error() {
            return ExecuteOutcome::Failed(info);
        }
        if self.outcome_indeterminate() {
            return ExecuteOutcome::Indeterminate(DriverErrorInfo {
                code: "device_unreachable".to_owned(),
                message: "命令下发后无法确认".to_owned(),
                protocol_code: None,
                retryable: true,
            });
        }
        ExecuteOutcome::Succeeded(RawCommandResult {
            success: true,
            protocol_code: Some(0),
            payload: None,
            error: None,
        })
    }
}

/// 恒失败的前置条件检查器（用于验证 §85 在 Driver 前拒绝）。
struct AlwaysFailChecker;

/// 可注入失败的 Journal（P1-1/P1-2：插入/结算失败路径）。
struct FailingJournal {
    inner: InMemoryJournal,
    fail_insert: AtomicBool,
    fail_settle: AtomicBool,
}

impl FailingJournal {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: InMemoryJournal::new(),
            fail_insert: AtomicBool::new(false),
            fail_settle: AtomicBool::new(false),
        })
    }

    fn fail_insert(&self, fail: bool) {
        self.fail_insert.store(fail, Ordering::SeqCst);
    }

    fn fail_settle(&self, fail: bool) {
        self.fail_settle.store(fail, Ordering::SeqCst);
    }
}

impl ControlJournal for FailingJournal {
    fn try_insert(
        &self,
        key: &IdempotencyKey,
        payload_hash: String,
        created_at_ns: observation_model::TimestampNs,
        expires_at_ns: observation_model::TimestampNs,
    ) -> Result<crate::journal::JournalDecision, crate::journal::JournalError> {
        if self.fail_insert.load(Ordering::SeqCst) {
            return Err(crate::journal::JournalError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "磁盘只读",
            )));
        }
        self.inner
            .try_insert(key, payload_hash, created_at_ns, expires_at_ns)
    }

    fn settle(
        &self,
        key: &IdempotencyKey,
        result: &observation_model::ControlResult,
    ) -> Result<(), JournalError> {
        if self.fail_settle.load(Ordering::SeqCst) {
            return Err(crate::journal::JournalError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "磁盘只读",
            )));
        }
        self.inner.settle(key, result)
    }

    fn get(&self, key: &IdempotencyKey) -> Option<crate::journal::JournalEntry> {
        self.inner.get(key)
    }

    fn purge_expired(&self, now_ns: observation_model::TimestampNs) -> usize {
        self.inner.purge_expired(now_ns)
    }
}

#[async_trait::async_trait]
impl PreconditionChecker for AlwaysFailChecker {
    async fn check(
        &self,
        _device_id: &DeviceId,
        _preconditions: &[observation_model::CommandPrecondition],
    ) -> Result<(), PreconditionError> {
        Err(PreconditionError {
            message: "前置条件不满足：drive.mode != auto".to_owned(),
        })
    }
}

async fn wait_for_calls(executor: &MockExecutor, n: usize) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while executor.call_count() < n {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("等待执行器调用超时");
}

/// 前置条件检查器：前 `pass_first` 次通过，其后失败（四审 P1 TOCTOU 测试）。
struct CountingChecker {
    remaining: AtomicUsize,
}

#[async_trait]
impl PreconditionChecker for CountingChecker {
    async fn check(
        &self,
        _device_id: &DeviceId,
        _preconditions: &[observation_model::CommandPrecondition],
    ) -> Result<(), PreconditionError> {
        let prev = self.remaining.fetch_sub(1, Ordering::SeqCst);
        if prev > 0 {
            Ok(())
        } else {
            Err(PreconditionError {
                message: "执行时前置条件不再满足".to_owned(),
            })
        }
    }
}

/// 前置条件检查器：固定延迟后通过（四审 P1 超时测试）。
struct SlowChecker {
    delay_ms: u64,
}

#[async_trait]
impl PreconditionChecker for SlowChecker {
    async fn check(
        &self,
        _device_id: &DeviceId,
        _preconditions: &[observation_model::CommandPrecondition],
    ) -> Result<(), PreconditionError> {
        tokio::time::sleep(Duration::from_millis(self.delay_ms)).await;
        Ok(())
    }
}

/// 慢审计输出（四审 P2：审计超时不阻塞控制）。
struct SlowAuditSink {
    delay_ms: u64,
}

#[async_trait]
impl crate::audit::AuditSink for SlowAuditSink {
    async fn record(&self, _event: crate::audit::AuditEvent) {
        tokio::time::sleep(Duration::from_millis(self.delay_ms)).await;
    }
}

fn context_for(subject: &str) -> SubmitContext {
    SubmitContext {
        subject: subject.to_owned(),
        source: "rest:127.0.0.1:53211".to_owned(),
    }
}

// ---- 装配辅助 ---------------------------------------------------------------

const NS: &str = "plant-a";
const DEV: &str = "dev-1";

fn context() -> SubmitContext {
    SubmitContext {
        subject: "alice".to_owned(),
        source: "rest:127.0.0.1:53211".to_owned(),
    }
}

fn catalog() -> Arc<MemoryDeviceCatalog> {
    let mut catalog = MemoryDeviceCatalog::new();
    catalog.insert_profile(DEV.to_owned(), profile_for_test());
    Arc::new(catalog)
}

fn authorizer(role: Role) -> Arc<MemoryAuthorizer> {
    let auth = MemoryAuthorizer::new();
    auth.set_role("alice", role);
    Arc::new(auth)
}

fn default_policy() -> Arc<ControlPolicy> {
    let mut policy = ControlPolicy::default();
    // 测试 Profile 的命令声明了前置条件（§85）：fail-closed 语义下
    // 未配置检查器会被拒绝，故默认挂载放行检查器；前置条件专项测试
    // 自行覆盖（AlwaysFailChecker / 无检查器）。
    policy.precondition_checker =
        Some(Arc::new(crate::precondition::PermissivePreconditionChecker));
    // 冷却期机制由专项测试覆盖（显式策略）；机制类测试关闭以避免
    // Indeterminate 结算后的冷却拒绝干扰后续提交。
    policy.indeterminate_cooldown_ms = 0;
    Arc::new(policy)
}

fn write_request(id: &str, path: &str, value: Value) -> ControlRequest {
    ControlRequest {
        request_id: id.to_owned(),
        namespace: NS.to_owned(),
        device_id: DEV.to_owned(),
        requested_at_ns: now_ns(),
        timeout_ms: 5_000,
        operation: ControlOperation::PropertyWrite(PropertyWriteRequest {
            items: vec![PropertyWriteItem {
                path: path.to_owned(),
                value,
            }],
        }),
    }
}

fn frequency_write(id: &str, value: f64) -> ControlRequest {
    write_request(id, "drive.output.frequency", Value::F64(value))
}

fn command_request(id: &str, ack: bool) -> ControlRequest {
    ControlRequest {
        request_id: id.to_owned(),
        namespace: NS.to_owned(),
        device_id: DEV.to_owned(),
        requested_at_ns: now_ns(),
        timeout_ms: 5_000,
        operation: ControlOperation::CommandExecute(CommandRequest {
            command: "drive.reset".to_owned(),
            parameters: vec![CommandParameter {
                name: "ack".to_owned(),
                value: Value::Bool(ack),
            }],
        }),
    }
}

fn engine_with(
    catalog: Arc<dyn DeviceCatalog>,
    auth: Arc<dyn Authorizer>,
    journal: Arc<dyn ControlJournal>,
    executor: Arc<dyn ControlExecutor>,
    audit: Arc<dyn crate::audit::AuditSink>,
    policy: Arc<ControlPolicy>,
) -> ControlEngine {
    ControlEngine::new(ControlEngineConfig {
        catalog,
        authorizer: auth,
        journal,
        executor,
        audit,
        policy,
    })
}

fn in_memory_engine(
    executor: Arc<dyn ControlExecutor>,
    audit: Arc<MemoryAuditSink>,
) -> ControlEngine {
    engine_with(
        catalog(),
        authorizer(Role::Operator),
        Arc::new(InMemoryJournal::new()),
        executor,
        audit,
        default_policy(),
    )
}

fn key(id: &str) -> IdempotencyKey {
    IdempotencyKey {
        namespace: NS.to_owned(),
        device_id: DEV.to_owned(),
        request_id: id.to_owned(),
    }
}

// ---- 测试 -------------------------------------------------------------------

/// 同一引擎统一处理属性写入与命令执行；结果回填完整信封标识（§89）。
#[tokio::test]
async fn same_engine_handles_write_and_command() {
    let executor = MockExecutor::new();
    executor.release();
    let audit = Arc::new(MemoryAuditSink::new());
    let engine = in_memory_engine(executor.clone(), audit.clone());

    let write = engine
        .submit(frequency_write("w-1", 50.0), &context())
        .await
        .unwrap()
        .wait()
        .await;
    assert_eq!(write.status, ControlStatus::Succeeded);
    assert_eq!(write.request_id, "w-1", "Done 路径须回填 request_id");
    assert_eq!(write.namespace, NS);
    assert_eq!(write.device_id, DEV);
    let Some(ControlPayloadResult::PropertyWrite(items)) = write.result else {
        panic!("属性写入结果应映射为 PropertyWriteItemResult");
    };
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].path, "drive.output.frequency");
    assert!(items[0].success);

    let cmd = engine
        .submit(command_request("c-1", true), &context())
        .await
        .unwrap()
        .wait()
        .await;
    assert_eq!(cmd.status, ControlStatus::Succeeded);
    assert_eq!(cmd.request_id, "c-1");
    assert_eq!(cmd.namespace, NS);
    let Some(ControlPayloadResult::Command(cmd_result)) = cmd.result else {
        panic!("命令结果应映射为 CommandResult");
    };
    assert_eq!(cmd_result.device_code, Some(0));

    // 写入地址由 Profile 映射（引擎不解析 Driver 地址，§10）。
    let calls = executor.calls();
    assert_eq!(calls.len(), 2);
    assert!(calls[0].starts_with("write:1!40001"), "实际 {calls:?}");
    assert!(calls[1].starts_with("cmd:reset"), "实际 {calls:?}");
}

/// 校验失败在 Driver 前以 Rejected 结算，执行器零调用（§84）。
#[tokio::test]
async fn validation_failures_rejected_before_driver() {
    let executor = MockExecutor::new();
    executor.release();
    let audit = Arc::new(MemoryAuditSink::new());
    let engine = in_memory_engine(executor.clone(), audit.clone());

    let mut cases: Vec<(String, ControlRequest, &str)> = vec![
        (
            "unknown-property".to_owned(),
            write_request("r-1", "drive.nonexistent", Value::F64(1.0)),
            "PROPERTY_NOT_FOUND",
        ),
        (
            "type-mismatch".to_owned(),
            write_request("r-2", "drive.output.frequency", Value::Bool(true)),
            "VALUE_TYPE_MISMATCH",
        ),
        (
            "out-of-range".to_owned(),
            frequency_write("r-3", 500.0),
            "VALUE_OUT_OF_RANGE",
        ),
        (
            "readonly".to_owned(),
            write_request("r-4", "drive.mode", Value::String("auto".to_owned())),
            "PROPERTY_NOT_WRITABLE",
        ),
        (
            "device-not-found".to_owned(),
            {
                let mut r = frequency_write("r-7", 50.0);
                r.device_id = "ghost".to_owned();
                r
            },
            "DEVICE_NOT_FOUND",
        ),
    ];
    let mut cmd_unknown = command_request("r-5", true);
    let ControlOperation::CommandExecute(c) = &mut cmd_unknown.operation else {
        unreachable!()
    };
    c.command = "drive.foo".to_owned();
    cases.push((
        "unknown-command".to_owned(),
        cmd_unknown,
        // P1-B：未授权用户（Operator 不足以执行 Critical 级命令）探测未知命令
        // 时不得泄露命令是否存在——统一以权限不足拒绝。
        "INSUFFICIENT_ROLE",
    ));
    let mut cmd_missing = command_request("r-6", true);
    let ControlOperation::CommandExecute(c) = &mut cmd_missing.operation else {
        unreachable!()
    };
    c.parameters.clear();
    cases.push(("missing-param".to_owned(), cmd_missing, "MISSING_PARAMETER"));

    for (name, request, expected_code) in cases {
        let receipt = engine.submit(request, &context()).await.unwrap();
        assert!(receipt.is_ready(), "{name}: 应即时拒绝");
        let result = receipt.wait().await;
        assert_eq!(result.status, ControlStatus::Rejected, "{name}");
        assert_eq!(
            result.error.unwrap().code,
            expected_code,
            "{name}: 错误码不符"
        );
    }
    assert_eq!(executor.call_count(), 0, "Driver 前拒绝：执行器不得被调用");

    // P1-B：已获授权（Administrator）的用户探测未知命令时才可见
    // COMMAND_NOT_FOUND（命令存在性不向未授权用户泄露）。
    let admin_executor = MockExecutor::new();
    admin_executor.release();
    let admin_engine = engine_with(
        catalog(),
        authorizer(Role::Administrator),
        Arc::new(InMemoryJournal::new()),
        admin_executor.clone(),
        Arc::new(MemoryAuditSink::new()),
        default_policy(),
    );
    let mut cmd_unknown_admin = command_request("r-8", true);
    let ControlOperation::CommandExecute(c) = &mut cmd_unknown_admin.operation else {
        unreachable!()
    };
    c.command = "drive.foo".to_owned();
    let receipt = admin_engine
        .submit(cmd_unknown_admin, &context())
        .await
        .unwrap();
    assert!(receipt.is_ready(), "已授权用户探测未知命令应即时拒绝");
    let result = receipt.wait().await;
    assert_eq!(result.status, ControlStatus::Rejected);
    assert_eq!(
        result.error.unwrap().code,
        "COMMAND_NOT_FOUND",
        "已授权用户的探测应得到 COMMAND_NOT_FOUND"
    );
    assert_eq!(admin_executor.call_count(), 0);
}

/// 越权（§83）与前置条件不满足（§85）在 Driver 前拒绝。
#[tokio::test]
async fn unauthorized_and_precondition_rejected_before_driver() {
    let executor = MockExecutor::new();
    executor.release();
    let audit = Arc::new(MemoryAuditSink::new());

    // Viewer 不能控制（§83）。
    let engine = engine_with(
        catalog(),
        authorizer(Role::Viewer),
        Arc::new(InMemoryJournal::new()),
        executor.clone(),
        audit.clone(),
        default_policy(),
    );
    let result = engine
        .submit(command_request("u-1", true), &context())
        .await
        .unwrap()
        .wait()
        .await;
    assert_eq!(result.status, ControlStatus::Rejected);
    assert_eq!(result.error.unwrap().code, "INSUFFICIENT_ROLE");

    // 前置条件失败（§85）。
    let mut policy = ControlPolicy::default();
    policy.precondition_checker = Some(Arc::new(AlwaysFailChecker));
    let engine2 = engine_with(
        catalog(),
        authorizer(Role::Operator),
        Arc::new(InMemoryJournal::new()),
        executor.clone(),
        audit.clone(),
        Arc::new(policy),
    );
    let result = engine2
        .submit(command_request("u-2", true), &context())
        .await
        .unwrap()
        .wait()
        .await;
    assert_eq!(result.status, ControlStatus::Rejected);
    assert_eq!(result.error.unwrap().code, "PRECONDITION_FAILED");

    assert_eq!(executor.call_count(), 0);
}

/// 同设备控制请求串行执行（§87：并发数恒为 1）。
#[tokio::test]
async fn per_device_serial_execution() {
    let executor = MockExecutor::new(); // 阻塞模式
    let audit = Arc::new(MemoryAuditSink::new());
    let engine = in_memory_engine(executor.clone(), audit.clone());

    let ctx = context();
    let r1 = engine.submit(frequency_write("s-1", 10.0), &ctx);
    let r2 = engine.submit(command_request("s-2", true), &ctx);
    let (r1, r2) = tokio::join!(r1, r2);
    let (receipt1, receipt2) = (r1.unwrap(), r2.unwrap());
    wait_for_calls(&executor, 1).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        executor.max_concurrent(),
        1,
        "同设备不得并发执行（§87 串行）"
    );
    executor.release();
    let r1 = receipt1.wait().await;
    let r2 = receipt2.wait().await;
    assert_eq!(r1.status, ControlStatus::Succeeded);
    assert_eq!(r2.status, ControlStatus::Succeeded);
}

/// 幂等：同 key + 同 payload → 直接返回既有结果，不重复执行（§80.1）。
#[tokio::test]
async fn idempotent_duplicate_returns_existing_result() {
    let executor = MockExecutor::new();
    executor.release();
    let audit = Arc::new(MemoryAuditSink::new());
    let engine = in_memory_engine(executor.clone(), audit.clone());

    let first = engine
        .submit(frequency_write("dup-1", 30.0), &context())
        .await
        .unwrap()
        .wait()
        .await;
    assert_eq!(first.status, ControlStatus::Succeeded);

    let receipt = engine
        .submit(frequency_write("dup-1", 30.0), &context())
        .await
        .unwrap();
    assert!(receipt.is_ready(), "幂等命中应即时返回");
    let second = receipt.wait().await;
    assert_eq!(second.status, ControlStatus::Succeeded);
    assert_eq!(executor.call_count(), 1, "幂等命中不得重复执行");

    // status() 查询（§77 轮询；四审 P1：三态区分）。
    let queried = engine
        .status(&key("dup-1"), &context())
        .await
        .expect("应可查询既有结果");
    match queried {
        StatusQuery::Settled(result) => {
            assert_eq!(result.status, ControlStatus::Succeeded);
        }
        other => panic!("已结算请求应返回 Settled，实际 {other:?}"),
    }
}

/// 幂等冲突：同 key + 不同 payload → SubmitError::Conflict（§80.1）。
#[tokio::test]
async fn idempotent_conflict_rejected() {
    let executor = MockExecutor::new();
    executor.release();
    let audit = Arc::new(MemoryAuditSink::new());
    let engine = in_memory_engine(executor.clone(), audit.clone());

    engine
        .submit(frequency_write("conf-1", 30.0), &context())
        .await
        .unwrap()
        .wait()
        .await;

    let err = match engine
        .submit(frequency_write("conf-1", 60.0), &context())
        .await
    {
        Err(e) => e,
        Ok(_) => panic!("同 key 不同 payload 应冲突"),
    };
    assert!(matches!(err, SubmitError::Conflict { .. }));
    assert_eq!(executor.call_count(), 1);
}

/// 重启恢复：未结算记录 → Indeterminate，且不重放执行（§80.1）。
#[test]
fn restart_recovers_indeterminate_without_replay() {
    let dir = std::env::temp_dir().join(format!(
        "forge-control-engine-{}-restart",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("journal.jsonl");

    let executor1 = MockExecutor::new(); // 永不放行 → 模拟进程在执行中死亡
    let audit1 = Arc::new(MemoryAuditSink::new());
    let engine1 = in_memory_engine_with_journal(executor1.clone(), audit1.clone(), &path);
    let rt1 = tokio::runtime::Runtime::new().unwrap();
    let receipt = rt1
        .block_on(engine1.submit(frequency_write("crash-1", 30.0), &context()))
        .unwrap();
    assert!(!receipt.is_ready());
    // 运行时销毁 = 进程崩溃：worker 任务被中止，记录停留在 Running。
    drop(rt1);

    // 重启：同 Journal + 全新引擎；幂等命中但结果不确定 → Indeterminate，不重放。
    let executor2 = MockExecutor::new();
    executor2.release();
    let audit2 = Arc::new(MemoryAuditSink::new());
    let engine2 = in_memory_engine_with_journal(executor2.clone(), audit2.clone(), &path);
    let rt2 = tokio::runtime::Runtime::new().unwrap();
    let result = rt2
        .block_on(engine2.submit(frequency_write("crash-1", 30.0), &context()))
        .unwrap()
        .wait();
    let result = rt2.block_on(result);
    assert_eq!(result.status, ControlStatus::Indeterminate);
    assert_eq!(
        result.error.unwrap().code,
        "EXECUTION_INTERRUPTED",
        "重启后不确定结果须显式表达，禁止盲目重放"
    );
    assert_eq!(executor2.call_count(), 0, "Indeterminate 不得重放执行");

    std::fs::remove_dir_all(&dir).unwrap();
}

fn in_memory_engine_with_journal(
    executor: Arc<dyn ControlExecutor>,
    audit: Arc<MemoryAuditSink>,
    path: &std::path::Path,
) -> ControlEngine {
    engine_with(
        catalog(),
        authorizer(Role::Operator),
        Arc::new(FileJournal::open(path, now_ns()).expect("打开 FileJournal")),
        executor,
        audit,
        default_policy(),
    )
}

/// 队列满：容量 1 时第三个请求（排队已占满）即时 Rejected QUEUE_FULL（§87 有界）。
#[tokio::test]
async fn queue_full_rejected() {
    let executor = MockExecutor::new(); // 阻塞第一个
    let audit = Arc::new(MemoryAuditSink::new());
    let mut policy = ControlPolicy::default();
    policy.queue_capacity = 1;
    let engine = engine_with(
        catalog(),
        authorizer(Role::Operator),
        Arc::new(InMemoryJournal::new()),
        executor.clone(),
        audit.clone(),
        Arc::new(policy),
    );

    // A 占用 worker（运行中，不计入队列容量）；B 入队占满容量 1。
    let a = engine
        .submit(frequency_write("q-1", 10.0), &context())
        .await
        .unwrap();
    wait_for_calls(&executor, 1).await;
    let b = engine
        .submit(frequency_write("q-2", 20.0), &context())
        .await
        .unwrap();
    assert!(!b.is_ready(), "B 应排队等待");

    // C：队列已满 → 即时拒绝。
    let c = engine
        .submit(frequency_write("q-3", 30.0), &context())
        .await
        .unwrap();
    assert!(c.is_ready(), "队列满应即时拒绝");
    let result = c.wait().await;
    assert_eq!(result.status, ControlStatus::Rejected);
    assert_eq!(result.error.unwrap().code, "QUEUE_FULL");

    executor.release();
    assert_eq!(a.wait().await.status, ControlStatus::Succeeded);
    executor.release();
    assert_eq!(b.wait().await.status, ControlStatus::Succeeded);
}

/// 超时：排队期间过期 → Timeout（执行器从未被轮询，§77）；
/// 执行中超时 → Indeterminate（驱动可能已下发，§80.1/P1-7）。
#[tokio::test]
async fn timeout_settles_timeout() {
    let executor = MockExecutor::new(); // 永不放行
    let audit = Arc::new(MemoryAuditSink::new());
    let engine = in_memory_engine(executor.clone(), audit.clone());

    // 执行中（执行器已开始）超时 → Indeterminate，不得宣称 Timeout（P1-7）。
    let mut request = frequency_write("t-1", 10.0);
    request.timeout_ms = 60;
    let result = engine
        .submit(request, &context())
        .await
        .unwrap()
        .wait()
        .await;
    assert_eq!(result.status, ControlStatus::Indeterminate);
    assert_eq!(result.error.unwrap().code, "TIMEOUT");
    assert_eq!(result.request_id, "t-1", "Timeout 路径须回填信封标识");
    assert_eq!(result.namespace, NS);

    // 排队期间过期（从未开始执行）→ Timeout。
    let holder = engine
        .submit(frequency_write("t-2", 20.0), &context())
        .await
        .unwrap();
    wait_for_calls(&executor, 2).await; // t-2 已进入执行器（阻塞中）
    let mut expired = frequency_write("t-3", 30.0);
    expired.timeout_ms = 20;
    let queued = engine.submit(expired, &context()).await.unwrap();
    // 等待 t-3 的截止时间过去后再放行 t-2。
    tokio::time::sleep(Duration::from_millis(50)).await;
    executor.release(); // t-2 完成 → worker 取 t-3（已过期 → Timeout）
    let result = queued.wait().await;
    assert_eq!(result.status, ControlStatus::Timeout);
    assert_eq!(result.error.unwrap().code, "TIMEOUT");
    assert_eq!(holder.wait().await.status, ControlStatus::Succeeded);
}

/// 取消：排队中的请求可取消 → Cancelled（从未执行，§87）；
/// 运行中的请求取消 → Indeterminate（驱动可能已下发，§80.1/P1-7）。
#[tokio::test]
async fn cancel_settles_cancelled() {
    let executor = MockExecutor::new(); // 阻塞运行中的请求
    let audit = Arc::new(MemoryAuditSink::new());
    let engine = in_memory_engine(executor.clone(), audit.clone());

    let receipt = engine
        .submit(command_request("x-1", true), &context())
        .await
        .unwrap();
    wait_for_calls(&executor, 1).await; // 已进入执行器（运行中）
    engine.cancel(&key("x-1"), &context()).await.unwrap();
    let result = receipt.wait().await;
    assert_eq!(result.status, ControlStatus::Indeterminate);
    assert_eq!(result.error.unwrap().code, "CANCELLED");
    assert_eq!(result.request_id, "x-1");

    // 排队中取消 → Cancelled。
    let holder = engine
        .submit(frequency_write("x-2", 20.0), &context())
        .await
        .unwrap();
    wait_for_calls(&executor, 2).await; // x-2 已进入执行器（阻塞中）
    let queued_receipt = engine
        .submit(command_request("x-3", true), &context())
        .await
        .unwrap();
    engine.cancel(&key("x-3"), &context()).await.unwrap();
    executor.release(); // x-2 完成 → worker 取 x-3（已取消 → Cancelled）
    let result = queued_receipt.wait().await;
    assert_eq!(result.status, ControlStatus::Cancelled);
    assert_eq!(result.error.unwrap().code, "CANCELLED");

    // 已结算：再次取消 → NotFound。
    assert_eq!(
        engine
            .cancel(&key("x-1"), &context())
            .await
            .unwrap_err()
            .to_string(),
        "请求不存在（已结算或未知）"
    );
    // 取消不存在的 key 不得误伤运行中的请求（P1-3）。
    let running = engine
        .submit(frequency_write("x-4", 40.0), &context())
        .await
        .unwrap();
    wait_for_calls(&executor, 3).await; // x-4 已进入执行器（阻塞中）
    assert_eq!(
        engine
            .cancel(&key("x-nonexistent"), &context())
            .await
            .unwrap_err()
            .to_string(),
        "请求不存在（已结算或未知）"
    );
    executor.release(); // x-2 完成
    assert_eq!(holder.wait().await.status, ControlStatus::Succeeded);
    executor.release(); // x-4 完成
    assert_eq!(running.wait().await.status, ControlStatus::Succeeded);
}

/// 优先级：高优先级命令先于低优先级写入执行（§87）。
#[tokio::test]
async fn priority_ordering() {
    let executor = MockExecutor::new(); // 阻塞，制造排队窗口
    let audit = Arc::new(MemoryAuditSink::new());
    let mut policy = ControlPolicy::default();
    policy.property_write_priority = Priority::Low;
    policy.command_priority = CommandPriority::from([(CommandRiskLevel::Medium, Priority::High)]);
    // 命令声明了前置条件（§85）：fail-closed 下需挂载放行检查器。
    policy.precondition_checker =
        Some(Arc::new(crate::precondition::PermissivePreconditionChecker));
    let engine = engine_with(
        catalog(),
        authorizer(Role::Operator),
        Arc::new(InMemoryJournal::new()),
        executor.clone(),
        audit.clone(),
        Arc::new(policy),
    );

    // 先入队一个占用 worker 的请求，再入队低/高优先级各一。
    let holder = engine
        .submit(frequency_write("p-1", 10.0), &context())
        .await
        .unwrap();
    wait_for_calls(&executor, 1).await;
    let low = engine
        .submit(frequency_write("p-2", 20.0), &context())
        .await
        .unwrap();
    let high = engine
        .submit(command_request("p-3", true), &context())
        .await
        .unwrap();

    executor.release(); // holder 完成 → worker 按优先级取下一个（应取 High）
    wait_for_calls(&executor, 2).await;
    executor.release(); // high 完成 → 取 low
    wait_for_calls(&executor, 3).await;
    executor.release(); // low 完成

    assert_eq!(holder.wait().await.status, ControlStatus::Succeeded);
    assert_eq!(high.wait().await.status, ControlStatus::Succeeded);
    assert_eq!(low.wait().await.status, ControlStatus::Succeeded);

    let calls = executor.calls();
    assert!(
        calls[1].starts_with("cmd:reset"),
        "高优先级应先执行: {calls:?}"
    );
    assert!(calls[2].starts_with("write:"), "低优先级后执行: {calls:?}");
}

/// 执行器明确失败 → Failed（错误码白名单）；结果不确定 → Indeterminate（§80.1）。
#[tokio::test]
async fn driver_failure_and_indeterminate_mapped() {
    let audit = Arc::new(MemoryAuditSink::new());

    // 明确失败：白名单内错误码透传。
    let executor = MockExecutor::new();
    executor.release();
    executor.set_fail(DriverErrorInfo {
        code: "timeout".to_owned(),
        message: "设备无应答".to_owned(),
        protocol_code: Some(3),
        retryable: true,
    });
    let engine = in_memory_engine(executor.clone(), audit.clone());
    let result = engine
        .submit(command_request("f-1", true), &context())
        .await
        .unwrap()
        .wait()
        .await;
    assert_eq!(result.status, ControlStatus::Failed);
    let err = result.error.unwrap();
    assert_eq!(err.code, "timeout");
    assert_eq!(err.details.unwrap()["protocol_code"], 3);

    // 结果不确定：禁止盲目重放（§80.1）。
    let executor2 = MockExecutor::new();
    executor2.release();
    executor2.set_indeterminate();
    let engine2 = in_memory_engine(executor2.clone(), audit);
    let result = engine2
        .submit(frequency_write("f-2", 30.0), &context())
        .await
        .unwrap()
        .wait()
        .await;
    assert_eq!(result.status, ControlStatus::Indeterminate);
    // 白名单外错误码归一为 driver_error（稳定码，不泄漏插件细节）。
    assert_eq!(result.error.unwrap().code, "driver_error");
}

/// 审计：成功/命令/拒绝/超时均记录，字段完整（§90）。
#[tokio::test]
async fn audit_records_events() {
    let executor = MockExecutor::new();
    executor.release();
    let audit = Arc::new(MemoryAuditSink::new());
    let engine = in_memory_engine(executor.clone(), audit.clone());

    engine
        .submit(frequency_write("a-1", 40.0), &context())
        .await
        .unwrap()
        .wait()
        .await;
    engine
        .submit(command_request("a-2", true), &context())
        .await
        .unwrap()
        .wait()
        .await;
    engine
        .submit(
            write_request("a-3", "drive.nonexistent", Value::F64(1.0)),
            &context(),
        )
        .await
        .unwrap()
        .wait()
        .await;

    let events = audit.events();
    assert_eq!(events.len(), 3);
    let write_ev = &events[0];
    assert_eq!(write_ev.operation, AuditOperation::PropertyWrite);
    assert_eq!(write_ev.status, ControlStatus::Succeeded);
    assert_eq!(write_ev.target, "drive.output.frequency");
    assert_eq!(write_ev.user, "alice");
    assert_eq!(write_ev.source, "rest:127.0.0.1:53211");
    assert_eq!(write_ev.namespace, NS);
    assert_eq!(write_ev.device_id, DEV);
    assert_eq!(write_ev.request_id, "a-1");
    assert_eq!(write_ev.error_code, None);

    let cmd_ev = &events[1];
    assert_eq!(cmd_ev.operation, AuditOperation::CommandExecute);
    assert_eq!(cmd_ev.status, ControlStatus::Succeeded);
    assert_eq!(cmd_ev.target, "drive.reset");
    assert_eq!(cmd_ev.risk_level, Some(CommandRiskLevel::Medium));

    let rej_ev = &events[2];
    assert_eq!(rej_ev.status, ControlStatus::Rejected);
    assert_eq!(rej_ev.error_code.as_deref(), Some("PROPERTY_NOT_FOUND"));
}

/// 停机：不再接收新请求；已入队请求排空结算（§93）。
#[tokio::test]
async fn shutdown_closes_engine() {
    let executor = MockExecutor::new();
    executor.release();
    let audit = Arc::new(MemoryAuditSink::new());
    let engine = in_memory_engine(executor.clone(), audit.clone());

    engine
        .submit(frequency_write("sh-1", 10.0), &context())
        .await
        .unwrap()
        .wait()
        .await;

    engine.shutdown(Duration::from_millis(500)).await;

    let err = match engine
        .submit(frequency_write("sh-2", 20.0), &context())
        .await
    {
        Err(e) => e,
        Ok(_) => panic!("停机后不得接收新请求"),
    };
    assert!(matches!(err, SubmitError::EngineClosed));
    assert!(engine.cancel(&key("sh-1"), &context()).await.is_err());
}

/// P1-1：幂等记录插入失败 → 拒绝（JOURNAL_UNAVAILABLE），执行器零调用
/// （缺记录时重试可能重复执行控制动作，必须在 Driver 前拦截）。
#[tokio::test]
async fn journal_insert_failure_rejects_before_driver() {
    let journal = FailingJournal::new();
    journal.fail_insert(true);
    let executor = MockExecutor::new();
    executor.release();
    let audit = Arc::new(MemoryAuditSink::new());
    let engine = engine_with(
        catalog(),
        authorizer(Role::Operator),
        journal,
        executor.clone(),
        audit.clone(),
        default_policy(),
    );

    let result = engine
        .submit(frequency_write("ji-1", 10.0), &context())
        .await
        .unwrap()
        .wait()
        .await;
    assert_eq!(result.status, ControlStatus::Rejected);
    assert_eq!(result.error.unwrap().code, "JOURNAL_UNAVAILABLE");
    assert_eq!(executor.call_count(), 0, "插入失败不得下发 Driver");
}

/// P1-2：幂等结算失败 → 结果降级 Indeterminate（当前进程与重启恢复一致，
/// 不向调用方宣称成功）。
#[tokio::test]
async fn journal_settle_failure_downgrades_to_indeterminate() {
    let journal = FailingJournal::new();
    let executor = MockExecutor::new();
    executor.release();
    let audit = Arc::new(MemoryAuditSink::new());
    let engine = engine_with(
        catalog(),
        authorizer(Role::Operator),
        journal.clone(),
        executor.clone(),
        audit.clone(),
        default_policy(),
    );

    // 结算失败：执行器成功，但幂等结算落盘失败 → 降级 Indeterminate，
    // 绝不向调用方宣称成功（否则重启恢复 Indeterminate 造成语义矛盾）。
    journal.fail_settle(true);
    let result = engine
        .submit(frequency_write("js-1", 10.0), &context())
        .await
        .unwrap()
        .wait()
        .await;
    assert_eq!(result.status, ControlStatus::Indeterminate);
    assert_eq!(result.error.unwrap().code, "JOURNAL_SETTLE_FAILED");
    assert_eq!(
        result.request_id, "js-1",
        "Indeterminate 路径仍须回填信封标识"
    );

    // 恢复后重提同 key（记录停留在 Running）→ 恢复为 Indeterminate，不重放。
    journal.fail_settle(false);
    let result = engine
        .submit(frequency_write("js-1", 10.0), &context())
        .await
        .unwrap()
        .wait()
        .await;
    assert_eq!(result.status, ControlStatus::Indeterminate);
    assert_eq!(result.error.unwrap().code, "EXECUTION_INTERRUPTED");
    assert_eq!(executor.call_count(), 1, "Indeterminate 不得重放执行");
}

/// 三审回归：Journal 仍为 Running 但无活跃执行者（结算落盘失败）时，
/// 并发重复提交不得挂起——新建 shared 的请求立即返回 EXECUTION_INTERRUPTED
/// 且必须先写入 shared 再移除 active，命中已有登记的等待者从 shared 取得
/// 同一结果（否则后者永久挂起）。
#[tokio::test]
async fn concurrent_duplicates_over_stale_running_record_resolve() {
    let journal = FailingJournal::new();
    let executor = MockExecutor::new();
    executor.release();
    let audit = Arc::new(MemoryAuditSink::new());
    let engine = engine_with(
        catalog(),
        authorizer(Role::Operator),
        journal.clone(),
        executor.clone(),
        audit.clone(),
        default_policy(),
    );

    // 首个执行：执行器成功但结算落盘失败 → Journal 停留在 Running、无活跃执行者。
    journal.fail_settle(true);
    let first = engine
        .submit(frequency_write("cd-1", 10.0), &context())
        .await
        .unwrap()
        .wait()
        .await;
    assert_eq!(first.status, ControlStatus::Indeterminate);
    assert_eq!(first.error.unwrap().code, "JOURNAL_SETTLE_FAILED");

    // 两个并发重复提交：一个新建 shared（fresh 分支），另一个命中已有登记
    // （pending 分支）——两条路径都必须有界完成。
    journal.fail_settle(false);
    let e2 = engine.clone();
    let e3 = engine.clone();
    let (r2, r3) = tokio::time::timeout(Duration::from_secs(5), async move {
        tokio::join!(
            async move {
                e2.submit(frequency_write("cd-1", 10.0), &context())
                    .await
                    .unwrap()
                    .wait()
                    .await
            },
            async move {
                e3.submit(frequency_write("cd-1", 10.0), &context())
                    .await
                    .unwrap()
                    .wait()
                    .await
            },
        )
    })
    .await
    .expect("并发重复提交不得挂起");
    for r in [r2, r3] {
        assert_eq!(r.status, ControlStatus::Indeterminate);
        assert_eq!(r.error.unwrap().code, "EXECUTION_INTERRUPTED");
    }
    assert_eq!(executor.call_count(), 1, "Indeterminate 不得重放执行");

    engine.shutdown(Duration::from_millis(500)).await;
}

/// P2-12：批量写入部分失败 → 顶层 Failed（北向状态自洽），逐项结果保留。
#[tokio::test]
async fn partial_write_failure_marks_top_level_failed() {
    let executor = MockExecutor::new();
    executor.release();
    executor.set_partial_fail();
    let audit = Arc::new(MemoryAuditSink::new());
    let engine = in_memory_engine(executor.clone(), audit.clone());

    // 两个写入项：id 0 成功、id 1 失败。
    let mut request = frequency_write("pw-1", 50.0);
    let ControlOperation::PropertyWrite(payload) = &mut request.operation else {
        unreachable!()
    };
    payload.items.push(PropertyWriteItem {
        path: "drive.output.frequency".to_owned(),
        value: Value::F64(60.0),
    });
    let result = engine
        .submit(request, &context())
        .await
        .unwrap()
        .wait()
        .await;
    assert_eq!(result.status, ControlStatus::Failed);
    assert_eq!(result.error.unwrap().code, "PARTIAL_WRITE_FAILURE");
    let Some(ControlPayloadResult::PropertyWrite(items)) = result.result else {
        panic!("应保留逐项结果");
    };
    assert_eq!(items.len(), 2);
    assert!(items[0].success);
    assert!(!items[1].success);
    assert_eq!(items[1].error.as_ref().unwrap().code, "modbus_exception");
}

/// P2-13：授权优先于前置条件——未授权用户不得触发设备状态检查（§83 → §85）。
#[tokio::test]
async fn authorization_precedes_precondition_check() {
    let executor = MockExecutor::new();
    executor.release();
    let audit = Arc::new(MemoryAuditSink::new());
    let mut policy = ControlPolicy::default();
    policy.precondition_checker = Some(Arc::new(AlwaysFailChecker));
    let engine = engine_with(
        catalog(),
        authorizer(Role::Viewer),
        Arc::new(InMemoryJournal::new()),
        executor.clone(),
        audit.clone(),
        Arc::new(policy),
    );

    let result = engine
        .submit(command_request("ap-1", true), &context())
        .await
        .unwrap()
        .wait()
        .await;
    assert_eq!(result.status, ControlStatus::Rejected);
    assert_eq!(
        result.error.unwrap().code,
        "INSUFFICIENT_ROLE",
        "未授权用户必须先被授权拒绝，不得触发前置条件（设备状态）检查"
    );
    assert_eq!(executor.call_count(), 0);
}

/// P1-9：策略校验——幂等保留期低于 24h 时装配引擎 panic（fail-fast）。
#[test]
#[should_panic(expected = "idempotency_retention 不得低于 24 小时")]
fn policy_validate_rejects_short_retention() {
    let mut policy = ControlPolicy::default();
    policy.idempotency_retention = Duration::from_secs(3600); // 1h
    assert!(policy.validate().is_err());
    let _ = ControlEngine::new(ControlEngineConfig {
        catalog: catalog(),
        authorizer: authorizer(Role::Operator),
        journal: Arc::new(InMemoryJournal::new()),
        executor: MockExecutor::new(),
        audit: Arc::new(MemoryAuditSink::new()),
        policy: Arc::new(policy),
    });
}

/// P1-4：强制中止（超时 join）结算遗留请求——运行中条目 Indeterminate、
/// 排队条目 Cancelled，收据不永久挂起、Journal 不残留 Running。
#[tokio::test]
async fn forced_abort_settles_abandoned_entries() {
    let executor = MockExecutor::new(); // 永不放行
    let audit = Arc::new(MemoryAuditSink::new());
    let engine = in_memory_engine(executor.clone(), audit.clone());

    let running = engine
        .submit(frequency_write("ab-1", 10.0), &context())
        .await
        .unwrap();
    wait_for_calls(&executor, 1).await; // 运行中（阻塞）
    let queued = engine
        .submit(frequency_write("ab-2", 20.0), &context())
        .await
        .unwrap();

    // 短 grace 触发强制中止：运行中 → Indeterminate、排队 → Cancelled。
    engine.shutdown(Duration::from_millis(20)).await;

    let run_result = tokio::time::timeout(Duration::from_secs(1), running.wait())
        .await
        .expect("运行中条目收据不得永久挂起");
    assert_eq!(run_result.status, ControlStatus::Indeterminate);
    assert_eq!(run_result.error.unwrap().code, "QUEUE_WORKER_ABORTED");

    let queued_result = tokio::time::timeout(Duration::from_secs(1), queued.wait())
        .await
        .expect("排队条目收据不得永久挂起");
    assert_eq!(queued_result.status, ControlStatus::Cancelled);

    // Journal 不残留 Running（均已结算）。
    let key = key("ab-1");
    let entry = engine
        .status(&key, &context())
        .await
        .expect("ab-1 应已结算");
    assert!(
        matches!(entry, StatusQuery::Settled(_)),
        "已中止请求应返回终态，实际 {entry:?}"
    );
}

/// 五审回归（P1）：停机排空阶段 worker 被强制中止时，已从 `QueueInner.entries`
/// 移出、尚未完成结算的排队条目不得丢失——收据必须就绪（不永久挂起）、
/// Journal 不得残留 Running。
///
/// 场景：A 执行返回 Indeterminate → 结算后设备进入冷却期，B/C/D 在 A 结算前
/// 已排队；停机使 worker 进入排空路径逐条结算（慢审计拖住单条结算），grace
/// 到期时强制中止恰好落在 B 的结算中间——此时 B/C/D 不能只存在于 worker
/// 本地状态，`settle_abandoned` 必须能从共享状态接管。
#[tokio::test]
async fn shutdown_drain_abort_does_not_lose_queued_receipts() {
    let executor = MockExecutor::new();
    executor.set_indeterminate(); // A 完成后返回 Indeterminate → 触发设备冷却期
    let audit = Arc::new(SlowAuditSink { delay_ms: 500 });
    // 冷却期把 worker 挡在调度外：B/C/D 得以在队列中等待停机排空。
    let engine = engine_with(
        catalog(),
        authorizer(Role::Operator),
        Arc::new(InMemoryJournal::new()),
        executor.clone(),
        audit.clone(),
        Arc::new(ControlPolicy {
            indeterminate_cooldown_ms: 30_000,
            ..ControlPolicy::default()
        }),
    );

    // A 阻塞执行期间 B/C/D 入队（冷却期尚未建立，入队不被拒）。
    let first = engine
        .submit(frequency_write("dr-1", 10.0), &context())
        .await
        .unwrap();
    wait_for_calls(&executor, 1).await;
    let mut queued = Vec::new();
    for id in ["dr-2", "dr-3", "dr-4"] {
        queued.push(
            engine
                .submit(frequency_write(id, 20.0), &context())
                .await
                .expect("B/C/D 应在冷却期建立前成功入队"),
        );
    }

    // A 完成（Indeterminate）→ 设备进入冷却期，worker 空转等待。
    executor.release();
    let first_result = tokio::time::timeout(Duration::from_secs(2), first.wait())
        .await
        .expect("首请求收据应就绪");
    assert_eq!(first_result.status, ControlStatus::Indeterminate);
    tokio::time::sleep(Duration::from_millis(50)).await; // worker 进入冷却等待

    // 停机：worker 排空 B/C/D，慢审计拖住单条结算 → grace 到期强制中止。
    engine.shutdown(Duration::from_millis(20)).await;

    for (index, receipt) in queued.into_iter().enumerate() {
        let result = tokio::time::timeout(Duration::from_secs(2), receipt.wait())
            .await
            .unwrap_or_else(|_| panic!("排队条目 {} 收据不得永久挂起", index + 2));
        assert_eq!(result.status, ControlStatus::Cancelled);
        assert_eq!(result.error.unwrap().code, "CANCELLED");
    }
    // 注：不在此断言 Journal 全部 Settled——强制中止后的结算受 S5 总预算
    // 约束，预算耗尽的条目按设计跳过落盘（Journal 停留 Running，重启恢复
    // 为 Indeterminate）；本测试只验证 P1 性质：收据不永久挂起。
}

/// 五审回归（P1 配套）：强制中止后的结算阶段，审计与 Journal 共享同一总
/// 预算（S5）——大量遗留条目 × 慢审计不得使停机总时长超过声明预算。
#[tokio::test]
async fn shutdown_settle_budget_bounds_audit_time() {
    let executor = MockExecutor::new();
    executor.set_indeterminate();
    let audit = Arc::new(SlowAuditSink { delay_ms: 800 });
    let engine = engine_with(
        catalog(),
        authorizer(Role::Operator),
        Arc::new(InMemoryJournal::new()),
        executor.clone(),
        audit.clone(),
        Arc::new(ControlPolicy {
            indeterminate_cooldown_ms: 30_000,
            ..ControlPolicy::default()
        }),
    );

    let first = engine
        .submit(frequency_write("bd-1", 10.0), &context())
        .await
        .unwrap();
    wait_for_calls(&executor, 1).await;
    let mut queued = Vec::new();
    for id in ["bd-2", "bd-3", "bd-4"] {
        queued.push(
            engine
                .submit(frequency_write(id, 20.0), &context())
                .await
                .expect("B/C/D 应在冷却期建立前成功入队"),
        );
    }
    executor.release();
    let first_result = tokio::time::timeout(Duration::from_secs(2), first.wait())
        .await
        .expect("首请求收据应就绪");
    assert_eq!(first_result.status, ControlStatus::Indeterminate);
    tokio::time::sleep(Duration::from_millis(50)).await;

    // grace=20ms → 结算预算 50ms；若审计不受预算约束，3×800ms ≈ 2.4s+。
    let start = std::time::Instant::now();
    engine.shutdown(Duration::from_millis(20)).await;
    let elapsed = start.elapsed();

    for receipt in queued {
        let result = tokio::time::timeout(Duration::from_secs(2), receipt.wait())
            .await
            .expect("收据不得永久挂起");
        assert_eq!(result.status, ControlStatus::Cancelled);
    }
    assert!(
        elapsed < Duration::from_millis(1_000),
        "停机结算应受总预算约束，实际 {elapsed:?}"
    );
}

/// 五审回归（P1）：强制中止落在"拾取即取消/过期"条目的结算中间时，该条目
/// 同样不得丢失（与排空路径同一共享可见不变量）。
#[tokio::test]
async fn forced_abort_during_picked_cancel_settle_keeps_receipt() {
    let executor = MockExecutor::new();
    let audit = Arc::new(SlowAuditSink { delay_ms: 500 });
    let engine = engine_with(
        catalog(),
        authorizer(Role::Operator),
        Arc::new(InMemoryJournal::new()),
        executor.clone(),
        audit.clone(),
        Arc::new(ControlPolicy {
            // 不用冷却期：B 以"已取消"被正常拾取。
            indeterminate_cooldown_ms: 0,
            ..ControlPolicy::default()
        }),
    );

    // A 阻塞执行期间 B 入队并被取消；A 完成后 worker 拾取 B 走 Cancelled
    // 结算路径，慢审计拖住结算——grace 到期强制中止落在该结算中间。
    let first = engine
        .submit(frequency_write("pc-1", 10.0), &context())
        .await
        .unwrap();
    wait_for_calls(&executor, 1).await;
    let second = engine
        .submit(frequency_write("pc-2", 20.0), &context())
        .await
        .unwrap();
    engine.cancel(&key("pc-2"), &context()).await.unwrap();
    executor.release();
    let first_result = tokio::time::timeout(Duration::from_secs(2), first.wait())
        .await
        .expect("首请求收据应就绪");
    assert_eq!(first_result.status, ControlStatus::Succeeded);
    tokio::time::sleep(Duration::from_millis(50)).await; // worker 拾取 B 并进入慢审计

    engine.shutdown(Duration::from_millis(20)).await;

    let second_result = tokio::time::timeout(Duration::from_secs(2), second.wait())
        .await
        .expect("被取消条目收据不得永久挂起");
    assert_eq!(second_result.status, ControlStatus::Cancelled);
}

/// P1-6：并发重复提交在首请求执行期间命中 active → 等待并共享同一结果，
/// 不得错误返回 EXECUTION_INTERRUPTED。
#[tokio::test]
async fn concurrent_duplicate_waits_for_first_result() {
    let executor = MockExecutor::new(); // 阻塞首个请求
    let audit = Arc::new(MemoryAuditSink::new());
    let engine = in_memory_engine(executor.clone(), audit.clone());

    let first = engine
        .submit(frequency_write("race-1", 10.0), &context())
        .await
        .unwrap();
    wait_for_calls(&executor, 1).await; // 首请求运行中

    // 并发重复提交（同 key 同 payload）→ 应等待共享结果而非 EXECUTION_INTERRUPTED。
    let dup = engine
        .submit(frequency_write("race-1", 10.0), &context())
        .await
        .unwrap();
    assert!(!dup.is_ready(), "执行中的幂等命中应等待首请求结果");

    executor.release();
    let r1 = first.wait().await;
    let r2 = dup.wait().await;
    assert_eq!(r1.status, ControlStatus::Succeeded);
    assert_eq!(r2.status, ControlStatus::Succeeded);
    assert_eq!(executor.call_count(), 1, "幂等命中不得重复执行");

    // active 无残留：重新提交同 key 已就绪（从 Journal 取结果，非 active）。
    let again = engine
        .submit(frequency_write("race-1", 10.0), &context())
        .await
        .unwrap();
    assert!(again.is_ready());
    assert_eq!(again.wait().await.status, ControlStatus::Succeeded);
}

/// P1-5：多设备停机并发 join——两个设备都阻塞时总停机时间接近 grace
/// 而非 2×grace（有界停机，§93）。
#[tokio::test]
async fn shutdown_joins_devices_concurrently() {
    let executor = MockExecutor::new(); // 永不放行
    let audit = Arc::new(MemoryAuditSink::new());

    let mut cat = MemoryDeviceCatalog::new();
    cat.insert_profile(DEV.to_owned(), profile_for_test());
    cat.insert_profile("dev-2".to_owned(), profile_for_test());
    let engine = engine_with(
        Arc::new(cat),
        authorizer(Role::Operator),
        Arc::new(InMemoryJournal::new()),
        executor.clone(),
        audit.clone(),
        default_policy(),
    );

    let mut r1 = frequency_write("d-1", 10.0);
    r1.device_id = DEV.to_owned();
    let mut r2 = frequency_write("d-2", 20.0);
    r2.device_id = "dev-2".to_owned();
    let p1 = engine.submit(r1, &context()).await.unwrap();
    let p2 = engine.submit(r2, &context()).await.unwrap();
    wait_for_calls(&executor, 2).await;

    let start = std::time::Instant::now();
    engine.shutdown(Duration::from_millis(30)).await;
    let elapsed = start.elapsed();

    // 两个 worker 都超时中止（约 30ms），总时长应远小于 2×grace（≈60ms+开销）。
    assert!(
        elapsed < Duration::from_millis(500),
        "并发 join 应保持有界停机: elapsed={elapsed:?}"
    );
    assert_eq!(p1.wait().await.status, ControlStatus::Indeterminate);
    assert_eq!(p2.wait().await.status, ControlStatus::Indeterminate);
}

/// P1-A：停机排空并 join 完成后提交必须 `EngineClosed`——即使
/// `get_or_create_queue` 会重建队列，新建队列共享引擎停机标志，入队拒绝，
/// 请求绝不在停机后被接受。
#[tokio::test]
async fn submit_after_shutdown_never_accepted() {
    let executor = MockExecutor::new(); // 阻塞运行中的请求
    let audit = Arc::new(MemoryAuditSink::new());
    let journal = Arc::new(InMemoryJournal::new());
    let engine = engine_with(
        catalog(),
        authorizer(Role::Operator),
        journal.clone(),
        executor.clone(),
        audit.clone(),
        default_policy(),
    );

    // 让一个请求在停机时处于运行中（强制中止路径）。
    let running = engine
        .submit(frequency_write("p1a-1", 10.0), &context())
        .await
        .unwrap();
    wait_for_calls(&executor, 1).await;
    engine.shutdown(Duration::from_millis(20)).await;
    executor.release();
    assert_eq!(
        running.wait().await.status,
        ControlStatus::Indeterminate,
        "运行中条目应被强制中止结算"
    );

    // 停机完成后提交：一律 EngineClosed，且不得留下任何 Journal 记录。
    let err = match engine
        .submit(frequency_write("p1a-2", 20.0), &context())
        .await
    {
        Err(e) => e,
        Ok(_) => panic!("停机后提交必须被拒绝"),
    };
    assert!(matches!(err, SubmitError::EngineClosed));
    assert!(
        journal.get(&key("p1a-2")).is_none(),
        "停机后拒绝的请求不得进入 Journal"
    );
    assert_eq!(executor.call_count(), 1, "停机后的请求不得触达执行器");
}

/// P1-A：与停机并发提交（覆盖"初始 closed 检查通过后停机排空、再入队"的
/// 竞态窗口）——任何结果都必须有界且 Journal 不残留 Running。
#[tokio::test]
async fn submit_concurrent_with_shutdown_leaves_no_running() {
    let executor = MockExecutor::new(); // 阻塞首个请求
    let audit = Arc::new(MemoryAuditSink::new());
    let journal = Arc::new(InMemoryJournal::new());
    let engine = engine_with(
        catalog(),
        authorizer(Role::Operator),
        journal.clone(),
        executor.clone(),
        audit.clone(),
        default_policy(),
    );

    let first = engine
        .submit(frequency_write("p1b-0", 10.0), &context())
        .await
        .unwrap();
    wait_for_calls(&executor, 1).await;

    let shutdown_engine = engine.clone();
    let shutdown = tokio::spawn(async move {
        shutdown_engine.shutdown(Duration::from_millis(30)).await;
    });
    let mut tasks = Vec::new();
    for i in 1..=10 {
        let engine = engine.clone();
        tasks.push(tokio::spawn(async move {
            let _ = engine
                .submit(frequency_write(&format!("p1b-{i}"), 10.0), &context())
                .await;
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    shutdown.await.unwrap();
    executor.release();
    let _ = first.wait().await;

    // 竞态窗口内任何被接受的请求都必须已结算；不得残留 Running。
    for i in 1..=10 {
        let k = key(&format!("p1b-{i}"));
        if let Some(entry) = journal.get(&k) {
            assert_ne!(
                entry.status,
                ControlStatus::Running,
                "停机窗口内的请求不得残留 Running: {k:?}"
            );
        }
    }
    assert_eq!(executor.call_count(), 1, "竞态窗口内的请求不得触达执行器");
}

/// P2-G：拒绝路径（校验失败，发生在幂等登记之后）结算落盘失败 → 结果降级
/// Indeterminate（JOURNAL_SETTLE_FAILED），不得宣称已以 Rejected 终态落盘。
/// （五审 P2 后：设备不存在/未授权等登记前拒绝不再写 Journal，无结算可言。）
#[tokio::test]
async fn reject_path_settle_failure_downgrades_to_indeterminate() {
    let journal = FailingJournal::new();
    journal.fail_settle(true);
    let executor = MockExecutor::new();
    executor.release();
    let audit = Arc::new(MemoryAuditSink::new());
    let engine = engine_with(
        catalog(),
        authorizer(Role::Operator),
        journal.clone(),
        executor.clone(),
        audit.clone(),
        default_policy(),
    );

    // 只读属性：通过登记（设备存在、已授权），在校验阶段被拒。
    let request = write_request("p2g-1", "drive.mode", Value::String("auto".to_owned()));
    let result = engine
        .submit(request, &context())
        .await
        .unwrap()
        .wait()
        .await;
    assert_eq!(result.status, ControlStatus::Indeterminate);
    assert_eq!(
        result.error.unwrap().code,
        "JOURNAL_SETTLE_FAILED",
        "拒绝结算失败必须降级为 Indeterminate"
    );
    assert_eq!(executor.call_count(), 0);
}

/// 四审 P1：Profile 声明前置条件但未配置检查器 → fail-closed 拒绝
/// （PRECONDITION_UNCONFIGURED），执行器零调用。
#[tokio::test]
async fn precondition_unconfigured_fails_closed() {
    let executor = MockExecutor::new();
    executor.release();
    let audit = Arc::new(MemoryAuditSink::new());
    let mut policy = ControlPolicy::default();
    policy.precondition_checker = None; // 未配置检查器
    let engine = engine_with(
        catalog(),
        authorizer(Role::Operator),
        Arc::new(InMemoryJournal::new()),
        executor.clone(),
        audit.clone(),
        Arc::new(policy),
    );

    let result = engine
        .submit(command_request("pc-1", true), &context())
        .await
        .unwrap()
        .wait()
        .await;
    assert_eq!(result.status, ControlStatus::Rejected);
    assert_eq!(result.error.unwrap().code, "PRECONDITION_UNCONFIGURED");
    assert_eq!(executor.call_count(), 0, "fail-closed 不得下发 Driver");
}

/// 四审 P1（TOCTOU）：提交时前置条件满足、执行时不再满足 → 在 Driver 前
/// 以 Failed(PRECONDITION_FAILED) 结算，执行器零调用。
#[tokio::test]
async fn precondition_rechecked_at_execution_time() {
    let executor = MockExecutor::new();
    executor.release();
    let audit = Arc::new(MemoryAuditSink::new());
    let mut policy = ControlPolicy::default();
    policy.precondition_checker = Some(Arc::new(CountingChecker {
        remaining: AtomicUsize::new(1), // 提交期通过，执行期复查失败
    }));
    let engine = engine_with(
        catalog(),
        authorizer(Role::Operator),
        Arc::new(InMemoryJournal::new()),
        executor.clone(),
        audit.clone(),
        Arc::new(policy),
    );

    let result = engine
        .submit(command_request("to-1", true), &context())
        .await
        .unwrap()
        .wait()
        .await;
    assert_eq!(result.status, ControlStatus::Failed);
    assert_eq!(result.error.unwrap().code, "PRECONDITION_FAILED");
    assert_eq!(executor.call_count(), 0, "复查失败必须在 Driver 前拦截");
}

/// 四审 P1：前置条件检查器卡死 → 受策略超时约束，以
/// PRECONDITION_TIMEOUT fail-closed 拒绝，不阻塞提交。
#[tokio::test]
async fn precondition_check_timeout_is_fail_closed() {
    let executor = MockExecutor::new();
    executor.release();
    let audit = Arc::new(MemoryAuditSink::new());
    let mut policy = ControlPolicy::default();
    policy.precondition_timeout_ms = 50;
    policy.precondition_checker = Some(Arc::new(SlowChecker { delay_ms: 5_000 }));
    let engine = engine_with(
        catalog(),
        authorizer(Role::Operator),
        Arc::new(InMemoryJournal::new()),
        executor.clone(),
        audit.clone(),
        Arc::new(policy),
    );

    let result = tokio::time::timeout(Duration::from_secs(2), async {
        engine
            .submit(command_request("pt-1", true), &context())
            .await
            .unwrap()
            .wait()
            .await
    })
    .await
    .expect("检查超时必须及时返回");
    assert_eq!(result.status, ControlStatus::Rejected);
    assert_eq!(result.error.unwrap().code, "PRECONDITION_TIMEOUT");
    assert_eq!(executor.call_count(), 0);
}

/// 四审 P1：status() 三态——执行中 Running / 未知 Unknown / 已结算 Settled。
#[tokio::test]
async fn status_distinguishes_running_unknown_settled() {
    let executor = MockExecutor::new(); // 阻塞
    let audit = Arc::new(MemoryAuditSink::new());
    let engine = in_memory_engine(executor.clone(), audit.clone());

    engine
        .submit(frequency_write("st-1", 10.0), &context())
        .await
        .unwrap();
    wait_for_calls(&executor, 1).await;
    // 执行中：Running（不是 None/Unknown）。
    match engine.status(&key("st-1"), &context()).await.unwrap() {
        StatusQuery::Running => {}
        other => panic!("执行中应返回 Running，实际 {other:?}"),
    }
    // 未知 key：Unknown。
    match engine.status(&key("st-none"), &context()).await.unwrap() {
        StatusQuery::Unknown => {}
        other => panic!("未知请求应返回 Unknown，实际 {other:?}"),
    }
    executor.release();
    let result = engine
        .submit(frequency_write("st-1", 10.0), &context())
        .await
        .unwrap()
        .wait()
        .await;
    assert_eq!(result.status, ControlStatus::Succeeded);
    match engine.status(&key("st-1"), &context()).await.unwrap() {
        StatusQuery::Settled(r) => assert_eq!(r.status, ControlStatus::Succeeded),
        other => panic!("已结算应返回 Settled，实际 {other:?}"),
    }
    engine.shutdown(Duration::from_millis(500)).await;
}

/// 四审 P1：cancel/status 需要授权上下文——低权限主体被拒绝。
#[tokio::test]
async fn cancel_and_status_require_authorization() {
    let executor = MockExecutor::new(); // 阻塞制造排队窗口
    let audit = Arc::new(MemoryAuditSink::new());
    let engine = in_memory_engine(executor.clone(), audit.clone());

    let receipt = engine
        .submit(frequency_write("au-1", 10.0), &context())
        .await
        .unwrap();
    wait_for_calls(&executor, 1).await;

    // 无角色主体：status 与 cancel 均拒绝。
    let err = engine
        .status(&key("au-1"), &context_for("mallory"))
        .await
        .unwrap_err();
    match err {
        crate::engine::StatusError::Unauthorized { code, .. } => {
            assert_eq!(code, "INSUFFICIENT_ROLE")
        }
    }
    let err = engine
        .cancel(&key("au-1"), &context_for("mallory"))
        .await
        .unwrap_err();
    assert!(
        matches!(err, crate::engine::CancelError::Unauthorized { .. }),
        "低权限取消应被拒绝，实际 {err:?}"
    );

    // 授权主体正常取消：取消令牌先触发，select! 以 Indeterminate 结算
    // （先断言再放行执行器，避免两分支同时就绪的随机选择）。
    engine.cancel(&key("au-1"), &context()).await.unwrap();
    let result = receipt.wait().await;
    assert_eq!(result.status, ControlStatus::Indeterminate);
    executor.release();
    engine.shutdown(Duration::from_millis(500)).await;
}

/// 四审 P2：requested_at_ns 过旧的请求以 REQUEST_TOO_OLD 拒绝。
#[tokio::test]
async fn stale_request_rejected() {
    let executor = MockExecutor::new();
    executor.release();
    let audit = Arc::new(MemoryAuditSink::new());
    let engine = in_memory_engine(executor.clone(), audit.clone());

    let mut request = frequency_write("old-1", 10.0);
    request.requested_at_ns = now_ns() - 10 * 60 * 1_000_000_000; // 10 分钟前
    let err = match engine.submit(request, &context()).await {
        Err(e) => e,
        Ok(_) => panic!("过旧请求应被拒绝"),
    };
    match err {
        SubmitError::InvalidRequest { code, .. } => assert_eq!(code, "REQUEST_TOO_OLD"),
        other => panic!("应为 InvalidRequest，实际 {other:?}"),
    }
    assert_eq!(executor.call_count(), 0);
}

/// 四审 P2：worker panic 后队列自动恢复——遗留请求以 Indeterminate 结算，
/// 新请求由重启的 worker 正常执行。
#[tokio::test]
async fn worker_panic_queue_recovers() {
    let executor = MockExecutor::new();
    executor.release();
    executor.set_panic_once();
    let audit = Arc::new(MemoryAuditSink::new());
    let engine = in_memory_engine(executor.clone(), audit.clone());

    // 首个请求触发执行器 panic → worker 任务终止（调用已计数）。
    let first = engine
        .submit(frequency_write("wp-1", 10.0), &context())
        .await
        .unwrap();
    wait_for_calls(&executor, 1).await;
    tokio::time::sleep(Duration::from_millis(50)).await; // 留出 panic 传播时间

    // 后续请求触发 ensure_worker 重启：遗留请求先结算，再执行新请求。
    let second = engine
        .submit(frequency_write("wp-2", 20.0), &context())
        .await
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), first.wait())
        .await
        .expect("panic 后收据不得永久挂起");
    assert_eq!(result.status, ControlStatus::Indeterminate);
    assert_eq!(result.error.unwrap().code, "QUEUE_WORKER_ABORTED");
    let second_result = tokio::time::timeout(Duration::from_secs(2), second.wait())
        .await
        .expect("重启后的 worker 应正常执行");
    assert_eq!(second_result.status, ControlStatus::Succeeded);
    engine.shutdown(Duration::from_millis(500)).await;
}

/// 五审回归（P1）：worker 在停机窗口内 panic——`join()` 收到
/// `Ok(Err(JoinError))` 时不得当作正常退出吞掉。此时 supervisor 因
/// `closed_flag` 已置位不再重启，若不调用 `settle_abandoned`，遗留的
/// running/draining 条目无人接管：收据永久挂起、Journal 残留 Running。
#[tokio::test]
async fn worker_panics_then_shutdown_settles_receipt() {
    let executor = MockExecutor::new();
    executor.release();
    executor.set_panic_once();
    let audit = Arc::new(MemoryAuditSink::new());
    let engine = in_memory_engine(executor.clone(), audit.clone());

    // 执行器 panic → worker 任务终止，running 条目遗留（收据未写）。
    let first = engine
        .submit(frequency_write("wp-1", 10.0), &context())
        .await
        .unwrap();
    wait_for_calls(&executor, 1).await;
    // 留出 panic 传播时间；须远小于 supervisor 的 500ms 轮询周期——
    // 保证停机时 worker 句柄仍是"已死未重启"状态。
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 停机：join 拿到已终止 worker 的句柄 → `Ok(Err(JoinError))`。
    engine.shutdown(Duration::from_millis(500)).await;

    let result = tokio::time::timeout(Duration::from_secs(2), first.wait())
        .await
        .expect("panic 遗留请求收据不得永久挂起");
    assert_eq!(result.status, ControlStatus::Indeterminate);
    assert_eq!(result.error.unwrap().code, "QUEUE_WORKER_ABORTED");
}

/// 四审 P2：慢审计不阻塞控制——审计写入超时后控制照常完成。
#[tokio::test]
async fn slow_audit_does_not_block_control() {
    let executor = MockExecutor::new();
    executor.release();
    let mut policy = ControlPolicy::default();
    policy.audit_timeout_ms = 20;
    let engine = engine_with(
        catalog(),
        authorizer(Role::Operator),
        Arc::new(InMemoryJournal::new()),
        executor.clone(),
        Arc::new(SlowAuditSink { delay_ms: 5_000 }),
        Arc::new(policy),
    );

    let started = std::time::Instant::now();
    let result = engine
        .submit(frequency_write("sa-1", 10.0), &context())
        .await
        .unwrap()
        .wait()
        .await;
    assert_eq!(result.status, ControlStatus::Succeeded);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "慢审计不得阻塞控制（耗时 {:?}）",
        started.elapsed()
    );
}

/// 四审 P2：审计摘要加盐——同一秘密的哈希前缀不再是裸 SHA-256
/// （低熵凭据无法离线字典枚举）。
#[test]
fn audit_secret_hash_is_salted() {
    use sha2::{Digest, Sha256};
    let secret = b"low-entropy-pin";
    let summarized =
        crate::audit::summarize_value(&Value::String(String::from_utf8(secret.to_vec()).unwrap()));
    let plain = format!("{:x}", Sha256::digest(secret));
    assert!(
        !summarized.summary.contains(&plain[..12]),
        "摘要不得等于裸 SHA-256 前缀（必须加盐）: {}",
        summarized.summary
    );
}

/// 四审 P2：优先级老化——低优先级请求排队超过阈值后晋升有效优先级，
/// 先于后到的高基础优先级请求执行（防饿死）。
#[tokio::test]
async fn priority_aging_prevents_starvation() {
    let executor = MockExecutor::new(); // 阻塞制造排队窗口
    let audit = Arc::new(MemoryAuditSink::new());
    let mut policy = ControlPolicy::default();
    policy.priority_aging_ms = 60;
    policy.property_write_priority = Priority::Low;
    policy.command_priority = CommandPriority::from([(CommandRiskLevel::Medium, Priority::High)]);
    policy.precondition_checker =
        Some(Arc::new(crate::precondition::PermissivePreconditionChecker));
    let engine = engine_with(
        catalog(),
        authorizer(Role::Operator),
        Arc::new(InMemoryJournal::new()),
        executor.clone(),
        audit.clone(),
        Arc::new(policy),
    );

    // 占用 worker（High，命令标签 cmd:...）。
    let holder = engine
        .submit(command_request("ag-h", true), &context())
        .await
        .unwrap();
    wait_for_calls(&executor, 1).await;
    // 低优先级写入排队并老化（250ms / 60ms → boost 3 → 有效 Critical）。
    let aged = engine
        .submit(frequency_write("ag-l", 10.0), &context())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;
    // 后到的高基础优先级命令（有效 High=2 < 老化后的 3）。
    let challenger = engine
        .submit(command_request("ag-m", false), &context())
        .await
        .unwrap();

    executor.release();
    wait_for_calls(&executor, 3).await;
    executor.release();
    let calls = executor.calls();
    // 老化的 Low 写入先于后到的 High 命令执行。
    assert!(
        calls[1].starts_with("write:"),
        "老化的低优先级写入应先执行: {calls:?}"
    );
    assert!(
        calls[2].starts_with("cmd:"),
        "后到的命令应后执行: {calls:?}"
    );
    let aged_result = tokio::time::timeout(Duration::from_secs(2), aged.wait())
        .await
        .expect("老化请求应完成");
    let challenger_result = tokio::time::timeout(Duration::from_secs(2), challenger.wait())
        .await
        .expect("挑战请求应完成");
    assert_eq!(aged_result.status, ControlStatus::Succeeded);
    assert_eq!(challenger_result.status, ControlStatus::Succeeded);
    assert_eq!(holder.wait().await.status, ControlStatus::Succeeded);
}

/// 四审 P1：同 key 异 payload 的 Conflict 不得留下无主 stale active 条目
/// ——否则后续同 key 重复提交会误判"命中活跃执行"而永久挂起。
#[tokio::test]
async fn conflict_over_stale_running_record_leaves_no_stale_active() {
    let journal = FailingJournal::new();
    let executor = MockExecutor::new();
    executor.release();
    let audit = Arc::new(MemoryAuditSink::new());
    let engine = engine_with(
        catalog(),
        authorizer(Role::Operator),
        journal.clone(),
        executor.clone(),
        audit.clone(),
        default_policy(),
    );

    // 首个执行：结算落盘失败 → Journal 停留 Running、无活跃执行者。
    journal.fail_settle(true);
    let first = engine
        .submit(command_request("cx-1", true), &context())
        .await
        .unwrap()
        .wait()
        .await;
    assert_eq!(first.error.unwrap().code, "JOURNAL_SETTLE_FAILED");
    journal.fail_settle(false);

    // 同 key 异 payload：Conflict（existing Running）——不得残留 active。
    let conflicted = engine
        .submit(command_request("cx-1", false), &context())
        .await;
    assert!(conflicted.is_err(), "异 payload 应返回 Conflict");

    // 同 key 同 payload 重提：必须立即返回 Indeterminate，不得挂起。
    let result = tokio::time::timeout(Duration::from_secs(2), async {
        engine
            .submit(command_request("cx-1", true), &context())
            .await
            .unwrap()
            .wait()
            .await
    })
    .await
    .expect("重提不得因 stale active 挂起");
    assert_eq!(result.status, ControlStatus::Indeterminate);
    assert_eq!(result.error.unwrap().code, "EXECUTION_INTERRUPTED");
    assert_eq!(
        executor.call_count(),
        1,
        "仅首个执行调用 Driver，重提不得重放"
    );
    engine.shutdown(Duration::from_millis(500)).await;
}

/// 五审 P1 测试替身：首次 `try_insert` 阻塞直到放行、然后失败；后续调用
/// 直接透传内存 Journal——用于构造"登记者 insert 失败、后来者 insert 成功"
/// 的领导权/所有权分离竞态。
struct RaceJournal {
    inner: InMemoryJournal,
    call_no: AtomicUsize,
    gate: Arc<AtomicBool>,
}

impl RaceJournal {
    fn new(gate: Arc<AtomicBool>) -> Arc<Self> {
        Arc::new(Self {
            inner: InMemoryJournal::new(),
            call_no: AtomicUsize::new(0),
            gate,
        })
    }
}

impl ControlJournal for RaceJournal {
    fn try_insert(
        &self,
        key: &IdempotencyKey,
        payload_hash: String,
        created_at_ns: observation_model::TimestampNs,
        expires_at_ns: observation_model::TimestampNs,
    ) -> Result<JournalDecision, JournalError> {
        let n = self.call_no.fetch_add(1, Ordering::SeqCst) + 1;
        if n == 1 {
            // 阻塞首次插入直到放行（spawn_blocking 线程内自旋等待，测试专用）。
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while !self.gate.load(Ordering::SeqCst) {
                if std::time::Instant::now() > deadline {
                    panic!("RaceJournal 门闩超时");
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            return Err(JournalError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "forced insert failure",
            )));
        }
        self.inner
            .try_insert(key, payload_hash, created_at_ns, expires_at_ns)
    }

    fn settle(
        &self,
        key: &IdempotencyKey,
        result: &observation_model::ControlResult,
    ) -> Result<(), JournalError> {
        self.inner.settle(key, result)
    }

    fn get(&self, key: &IdempotencyKey) -> Option<crate::journal::JournalEntry> {
        self.inner.get(key)
    }

    fn purge_expired(&self, now_ns: observation_model::TimestampNs) -> usize {
        self.inner.purge_expired(now_ns)
    }
}

/// 五审 P1：幂等并发竞态——登记者（fresh）的 insert 失败、后来者的 insert
/// 成功时，后来者不得执行设备动作（否则"客户端收到拒绝，动作却在执行"）。
#[tokio::test]
async fn idempotency_race_loser_does_not_execute() {
    let gate = Arc::new(AtomicBool::new(false));
    let journal = RaceJournal::new(gate.clone());
    let executor = MockExecutor::new();
    executor.release();
    let audit = Arc::new(MemoryAuditSink::new());
    let engine = engine_with(
        catalog(),
        authorizer(Role::Operator),
        journal,
        executor.clone(),
        audit.clone(),
        default_policy(),
    );

    // A 先登记 active（fresh），其 insert 被门闩阻塞。
    let a = tokio::spawn({
        let engine = engine.clone();
        async move {
            engine
                .submit(frequency_write("race-9", 10.0), &context())
                .await
                .unwrap()
                .wait()
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(100)).await; // 等 A 完成登记并进入 insert

    // B 后登记（非 fresh），其 insert 立即成功 → Inserted && !fresh。
    let b = engine
        .submit(frequency_write("race-9", 10.0), &context())
        .await
        .unwrap();
    let b_result = tokio::time::timeout(Duration::from_secs(2), b.wait())
        .await
        .expect("B 应及时返回");
    assert_eq!(b_result.status, ControlStatus::Rejected);
    assert_eq!(
        b_result.error.unwrap().code,
        "IDEMPOTENCY_RACE",
        "失去领导权的提交不得执行"
    );

    // 放行 A 的 insert（失败）→ A 以 JOURNAL_UNAVAILABLE 拒绝。
    gate.store(true, Ordering::SeqCst);
    let a_result = tokio::time::timeout(Duration::from_secs(2), a)
        .await
        .expect("A 应及时返回")
        .expect("A 任务不应 panic");
    assert_eq!(a_result.status, ControlStatus::Rejected);

    // 关键不变式：设备动作零执行。
    assert_eq!(executor.call_count(), 0, "领导权竞争双方都不得触达 Driver");
    engine.shutdown(Duration::from_millis(500)).await;
}

/// 五审 P1：授权先于幂等登记——未授权请求不占用 request_id（Journal 无
/// 记录）、合法用户随后可正常使用同一 request_id。
#[tokio::test]
async fn unauthorized_cannot_occupy_request_id() {
    let journal = Arc::new(InMemoryJournal::new());
    let executor = MockExecutor::new();
    executor.release();
    let audit = Arc::new(MemoryAuditSink::new());
    let auth = MemoryAuthorizer::new();
    auth.set_role("alice", Role::Operator); // mallory 无角色
    let engine = engine_with(
        catalog(),
        Arc::new(auth),
        journal.clone(),
        executor.clone(),
        audit.clone(),
        default_policy(),
    );

    // 未授权用户提交 → INSUFFICIENT_ROLE，且不得在 Journal 留痕。
    let err = engine
        .submit(frequency_write("occ-1", 10.0), &context_for("mallory"))
        .await
        .unwrap()
        .wait()
        .await;
    assert_eq!(err.status, ControlStatus::Rejected);
    assert_eq!(err.error.unwrap().code, "INSUFFICIENT_ROLE");
    assert!(
        journal.get(&key("occ-1")).is_none(),
        "未授权请求不得占用 request_id（Journal 无记录）"
    );

    // 合法用户使用同一 request_id 正常执行（不受占用影响）。
    let result = engine
        .submit(frequency_write("occ-1", 10.0), &context())
        .await
        .unwrap()
        .wait()
        .await;
    assert_eq!(result.status, ControlStatus::Succeeded);
    assert_eq!(executor.call_count(), 1);
}

/// 五审 P1：停机开始后不得启动新动作——排队条目就地 Cancelled，
/// 仅允许已在执行的条目自然完成。
#[tokio::test]
async fn shutdown_does_not_start_queued_actions() {
    let executor = MockExecutor::new(); // 阻塞制造排队窗口
    let audit = Arc::new(MemoryAuditSink::new());
    let engine = in_memory_engine(executor.clone(), audit.clone());

    let holder = engine
        .submit(frequency_write("sd-h", 10.0), &context())
        .await
        .unwrap();
    wait_for_calls(&executor, 1).await;
    let q1 = engine
        .submit(command_request("sd-1", true), &context())
        .await
        .unwrap();
    let q2 = engine
        .submit(frequency_write("sd-2", 20.0), &context())
        .await
        .unwrap();

    // 长 grace 停机（旧实现会在 holder 完成后继续下发排队命令）。
    let shutdown = tokio::spawn({
        let engine = engine.clone();
        async move { engine.shutdown(Duration::from_secs(5)).await }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    executor.release(); // holder 完成 → 排队条目必须被取消而非下发

    let (holder_result, q1_result, q2_result) = tokio::join!(holder.wait(), q1.wait(), q2.wait());
    assert_eq!(holder_result.status, ControlStatus::Succeeded);
    assert_eq!(q1_result.status, ControlStatus::Cancelled);
    assert_eq!(q2_result.status, ControlStatus::Cancelled);
    assert_eq!(executor.call_count(), 1, "停机后排队命令不得下发给 Driver");
    shutdown.await.unwrap();
}

/// 五审 P1：不确定结果触发设备冷却期——冷却期内新动作被拒绝，
/// 冷却结束后恢复。
#[tokio::test]
async fn indeterminate_triggers_device_cooldown() {
    let executor = MockExecutor::new(); // 阻塞
    let audit = Arc::new(MemoryAuditSink::new());
    let mut policy = ControlPolicy::default();
    policy.indeterminate_cooldown_ms = 300;
    let engine = engine_with(
        catalog(),
        authorizer(Role::Operator),
        Arc::new(InMemoryJournal::new()),
        executor.clone(),
        audit.clone(),
        Arc::new(policy),
    );

    let receipt = engine
        .submit(frequency_write("cd-a", 10.0), &context())
        .await
        .unwrap();
    wait_for_calls(&executor, 1).await;
    engine.cancel(&key("cd-a"), &context()).await.unwrap();
    let result = receipt.wait().await;
    assert_eq!(result.status, ControlStatus::Indeterminate);

    // 冷却期内：新动作拒绝。
    let rejected = engine
        .submit(frequency_write("cd-b", 20.0), &context())
        .await
        .unwrap()
        .wait()
        .await;
    assert_eq!(rejected.status, ControlStatus::Rejected);
    assert_eq!(rejected.error.unwrap().code, "DEVICE_COOLDOWN");

    // 冷却结束后：恢复正常。
    tokio::time::sleep(Duration::from_millis(400)).await;
    executor.release();
    let ok = engine
        .submit(frequency_write("cd-c", 30.0), &context())
        .await
        .unwrap()
        .wait()
        .await;
    assert_eq!(ok.status, ControlStatus::Succeeded);
    engine.shutdown(Duration::from_millis(500)).await;
}

/// 五审 S1：远未来时间戳拒绝（REQUEST_TOO_NEW）——防止绕过新鲜度校验。
#[tokio::test]
async fn future_timestamp_rejected() {
    let executor = MockExecutor::new();
    executor.release();
    let audit = Arc::new(MemoryAuditSink::new());
    let engine = in_memory_engine(executor.clone(), audit.clone());

    let mut request = frequency_write("ft-1", 10.0);
    request.requested_at_ns = now_ns() + 60 * 60 * 1_000_000_000; // 1 小时后
    let err = match engine.submit(request, &context()).await {
        Err(e) => e,
        Ok(_) => panic!("远未来时间戳应被拒绝"),
    };
    match err {
        SubmitError::InvalidRequest { code, .. } => assert_eq!(code, "REQUEST_TOO_NEW"),
        other => panic!("应为 InvalidRequest，实际 {other:?}"),
    }
}

/// 五审 S4：审计写入超时重试一次——慢一次的审计最终落库，控制不受阻塞。
#[tokio::test]
async fn audit_timeout_retries_once() {
    use crate::audit::{AuditEvent, AuditSink};
    use std::sync::atomic::AtomicUsize;

    struct SlowOnceAuditSink {
        calls: AtomicUsize,
    }
    #[async_trait]
    impl AuditSink for SlowOnceAuditSink {
        async fn record(&self, _event: AuditEvent) {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(300)).await; // 首次慢
            }
        }
    }

    let executor = MockExecutor::new();
    executor.release();
    let sink = Arc::new(SlowOnceAuditSink {
        calls: AtomicUsize::new(0),
    });
    let mut policy = ControlPolicy::default();
    policy.audit_timeout_ms = 50;
    let engine = engine_with(
        catalog(),
        authorizer(Role::Operator),
        Arc::new(InMemoryJournal::new()),
        executor.clone(),
        sink.clone(),
        Arc::new(policy),
    );

    let started = std::time::Instant::now();
    let result = engine
        .submit(frequency_write("ar-1", 10.0), &context())
        .await
        .unwrap()
        .wait()
        .await;
    assert_eq!(result.status, ControlStatus::Succeeded);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "重试不得阻塞控制（耗时 {:?}）",
        started.elapsed()
    );
    assert!(
        sink.calls.load(Ordering::SeqCst) >= 2,
        "超时后应至少重试一次"
    );
}
