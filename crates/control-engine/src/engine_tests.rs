//! Control Engine 集成测试（§81-§90 验收）。
//!
//! 覆盖：写/命令统一入口、Driver 前拒绝（校验/授权/前置条件）、每设备串行、
//! 幂等 Duplicate/Conflict 与重启恢复（Indeterminate 不重放）、队列满、
//! 超时、取消、优先级、审计、停机语义。

use std::sync::atomic::{AtomicUsize, Ordering};
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
use crate::engine::{ControlEngine, ControlEngineConfig, SubmitContext, SubmitError};
use crate::executor::{ControlExecutor, ExecuteOutcome, WriteOutcome};
use crate::journal::{ControlJournal, FileJournal, IdempotencyKey, InMemoryJournal};
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
            release,
            _keep_rx: rx,
        })
    }

    fn set_fail(&self, info: DriverErrorInfo) {
        *self.fail.write().expect("fail 锁被毒化") = Some(info);
    }

    fn set_indeterminate(&self) {
        *self.indeterminate.write().expect("indeterminate 锁被毒化") = true;
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

impl PreconditionChecker for AlwaysFailChecker {
    fn check(
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
    Arc::new(ControlPolicy::default())
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
        "COMMAND_NOT_FOUND",
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

    // status() 查询（§77 轮询）。
    let queried = engine.status(&key("dup-1")).expect("应可查询既有结果");
    assert_eq!(queried.status, ControlStatus::Succeeded);
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

/// 超时：执行器阻塞超过请求超时 → Timeout（§77、§80.1）。
#[tokio::test]
async fn timeout_settles_timeout() {
    let executor = MockExecutor::new(); // 永不放行
    let audit = Arc::new(MemoryAuditSink::new());
    let engine = in_memory_engine(executor.clone(), audit.clone());

    let mut request = frequency_write("t-1", 10.0);
    request.timeout_ms = 60;
    let result = engine
        .submit(request, &context())
        .await
        .unwrap()
        .wait()
        .await;
    assert_eq!(result.status, ControlStatus::Timeout);
    assert_eq!(result.error.unwrap().code, "TIMEOUT");
    assert_eq!(result.request_id, "t-1", "Timeout 路径须回填信封标识");
    assert_eq!(result.namespace, NS);
}

/// 取消：排队中与运行中的请求均可取消 → Cancelled（§87）。
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
    engine.cancel(&key("x-1")).await.unwrap();
    let result = receipt.wait().await;
    assert_eq!(result.status, ControlStatus::Cancelled);
    assert_eq!(result.error.unwrap().code, "CANCELLED");
    assert_eq!(result.request_id, "x-1");

    // 已结算：再次取消 → NotFound。
    assert_eq!(
        engine.cancel(&key("x-1")).await.unwrap_err().to_string(),
        "请求不存在（已结算或未知）"
    );
}

/// 优先级：高优先级命令先于低优先级写入执行（§87）。
#[tokio::test]
async fn priority_ordering() {
    let executor = MockExecutor::new(); // 阻塞，制造排队窗口
    let audit = Arc::new(MemoryAuditSink::new());
    let mut policy = ControlPolicy::default();
    policy.property_write_priority = Priority::Low;
    policy.command_priority = CommandPriority::from([(CommandRiskLevel::Medium, Priority::High)]);
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
    assert!(engine.cancel(&key("sh-1")).await.is_err());
}
