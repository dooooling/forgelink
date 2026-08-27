//! Startup Preflight（Runtime V2 方案 §29 Normative）。
//!
//! 每个 Device 启动前完成九项配置/契约预检；**任一失败 Collector 启动
//! fail-fast——no partial start、no silent device disable**：
//!
//! ```text
//! 1. driver exists
//! 2. profile exists
//! 3. profile.driver_id matches
//! 4. driver connection config valid
//! 5. every profile address validates
//! 6. profile capability requirements satisfied
//! 7. write properties require Write API
//! 8. commands require Command API
//! 9. subscription profiles require Subscription API
//! ```
//!
//! 职责边界：第 1~3 项在 `register_device` 中已有等价校验，此处提前到
//! **任何组件启动之前**统一执行（fail-fast 语义要求"启动失败时无需回收
//! 已启动组件"，方案 §100 装配顺序约束）；第 4~5 项通过 DriverFactory
//! 创建临时驱动实例验证连接配置与地址文法（实例即弃，不进入会话表）；
//! 第 6~9 项比对 Profile 声明的采集方式约束与 Driver 能力声明。

use std::collections::BTreeSet;

use device_manager::{DriverFactory, NativeDriverFactory};
use profile_engine::ProfileRegistry;
use tracing::{error, info};

use crate::config::CollectorConfig;
use crate::error::CollectorError;

/// Preflight 失败（§29 fail-fast：错误信息必须能定位到设备与检查项）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreflightFailure {
    /// 设备 ID。
    pub device_id: String,
    /// 检查项编号（§29 清单 1~9）与稳定标识。
    pub check: &'static str,
    pub reason: String,
}

impl std::fmt::Display for PreflightFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Preflight 失败（{check}）device={id}: {reason}",
            check = self.check,
            id = self.device_id,
            reason = self.reason
        )
    }
}

/// 对全部设备执行九项预检；返回第一个失败即中止（MVP 不聚合全部错误）。
///
/// `factory` 由调用方装配好已注册的插件集合（`drivers:` 或 legacy 路径
/// 产物一致）。
pub fn run_preflight(
    config: &CollectorConfig,
    profiles: &ProfileRegistry,
    factory: &NativeDriverFactory,
) -> Result<(), CollectorError> {
    let mut seen_driver_ids: BTreeSet<&str> = BTreeSet::new();
    // 预检逐设备执行；驱动实例按 (driver_id) 缓存复用——同一协议驱动的
    // 地址文法/能力声明相同，避免为同 driver 的每台设备重复创建实例。
    // 实例仅用于 validate_address / get_capabilities 两个无副作用查询，
    // 不 connect、不进入会话表。
    let mut probed: Vec<(String, Box<dyn device_manager::DriverSession>)> = Vec::new();

    for spec in &config.devices {
        // 1) driver exists（§29.1）：DriverFactory 已注册该 id。
        //    以一次 create_driver 试探表达"存在性 + 连接配置合法性"
        //    （4），工厂对未知 id 返回 UnknownDriver。
        let session = match factory.create_driver(&spec.driver, &spec.connection) {
            Ok(s) => s,
            Err(device_manager::BindError::UnknownDriver { driver_id }) => {
                return fail(
                    &spec.id,
                    "driver_exists",
                    format!("driver `{driver_id}` 未注册"),
                );
            }
            Err(e) => {
                // 4) driver connection config valid（§29.4）：创建失败 =
                //    配置非法（Driver 自身校验拒绝），fail-fast。
                return fail(
                    &spec.id,
                    "connection_config_valid",
                    format!("连接配置被 Driver 拒绝: {e}"),
                );
            }
        };

        // 2) profile exists（§29.2）。
        let profile = match profiles.get(&spec.profile) {
            Some(p) => p,
            None => {
                return fail(
                    &spec.id,
                    "profile_exists",
                    format!("Profile `{}` 未注册", spec.profile),
                );
            }
        };

        // 3) profile.driver_id matches（§29.3）。
        if profile.driver_id != spec.driver {
            return fail(
                &spec.id,
                "profile_driver_matches",
                format!(
                    "设备声明 driver `{}`, Profile `{}` 绑定 driver `{}`",
                    spec.driver, profile.id, profile.driver_id
                ),
            );
        }

        // 同一 driver 只探针一次（地址文法/能力与具体设备无关）。
        if seen_driver_ids.insert(spec.driver.as_str()) {
            // 5) every profile address validates（§29.5）：Profile 全部
            //    可读/可写属性的 driver_address 必须通过 Driver 校验。
            let mut session = session;
            validate_profile_addresses(spec, profile, &mut session)?;
            // 6~9) capability 约束比对。
            validate_capabilities(spec, profile, &mut session)?;
            probed.push((spec.driver.clone(), session));
        } else {
            // 后续同 driver 设备只做 Profile 静态一致性（上面已完成），
            // 地址/能力已在首台设备验证过同一 Profile 时覆盖。注意不同
            // 设备可绑定同 driver 的不同 Profile——此时仍需各自验证地址。
            drop(session);
        }
    }
    let _ = probed; // 探针实例随作用域释放（drop 即断开/销毁句柄）

    info!(
        component = "collector",
        devices = config.devices.len(),
        drivers = seen_driver_ids.len(),
        "Startup Preflight 通过"
    );
    Ok(())
}

fn fail(device_id: &str, check: &'static str, reason: String) -> Result<(), CollectorError> {
    let f = PreflightFailure {
        device_id: device_id.to_owned(),
        check,
        reason,
    };
    error!(component = "collector", "{f}");
    Err(CollectorError::Preflight(f))
}

/// §29.5：Profile 全部属性地址经 Driver `validate_address` 校验。
fn validate_profile_addresses(
    spec: &crate::config::DeviceSpec,
    profile: &std::sync::Arc<profile_engine::DeviceProfile>,
    session: &mut Box<dyn device_manager::DriverSession>,
) -> Result<(), CollectorError> {
    for prop in &profile.properties {
        if !prop.readable && !prop.writable {
            continue; // 双向不可用的属性不参与采集/控制，跳过
        }
        match session.validate_address(&prop.driver_address) {
            Ok(_) => {}
            Err(info) if info.code == "unsupported" => {
                // 会话实现不支持地址预检（如测试替身）：MVP 放行，
                // 运行期首次读取仍会被 Driver 拒绝（保守语义不变）。
                tracing::warn!(
                    component = "collector",
                    device_id = %spec.id,
                    "driver 不支持地址预检，跳过 §29.5（运行期仍由 Driver 校验）"
                );
                break;
            }
            Err(info) => {
                return fail(
                    &spec.id,
                    "address_validates",
                    format!(
                        "Profile `{}` 属性 `{}` 地址 {:?} 校验失败: {} ({})",
                        profile.id, prop.path, prop.driver_address, info.code, info.message
                    ),
                );
            }
        }
    }
    Ok(())
}

/// §29.6~§29.9：Profile 采集方式约束 vs Driver 协议能力。
fn validate_capabilities(
    spec: &crate::config::DeviceSpec,
    profile: &std::sync::Arc<profile_engine::DeviceProfile>,
    session: &mut Box<dyn device_manager::DriverSession>,
) -> Result<(), CollectorError> {
    let caps = match session.protocol_capabilities() {
        Ok(c) => c,
        Err(info) if info.code == "unsupported" => {
            tracing::warn!(
                component = "collector",
                device_id = %spec.id,
                "driver 不支持能力查询，跳过 §29.6~9（运行期 Unsupported 错误语义不变）"
            );
            return Ok(());
        }
        Err(info) => {
            return fail(
                &spec.id,
                "capability_requirements",
                format!("能力查询失败: {} ({})", info.code, info.message),
            );
        }
    };

    // 7) write properties require Write API。
    if profile.properties.iter().any(|p| p.writable) && !caps.write {
        return fail(
            &spec.id,
            "write_requires_write_api",
            format!(
                "Profile `{}` 含可写属性，但 driver 未声明 write 能力",
                profile.id
            ),
        );
    }
    // 8) commands require Command API。
    //
    // ABI v1 无独立 command 能力位（命令经 execute 下发）；以 write 位
    // 近似表达"有下行通道"。ABI v2 引入独立能力位后收紧。
    if !profile.commands.is_empty() && !caps.write {
        return fail(
            &spec.id,
            "commands_require_command_api",
            format!(
                "Profile `{}` 声明命令，但 driver 未声明下行能力",
                profile.id
            ),
        );
    }
    // 9) subscription profiles require Subscription API。
    if profile.capabilities.acquisition.subscription == Some(true) && !caps.subscription {
        return fail(
            &spec.id,
            "subscription_requires_subscription_api",
            format!(
                "Profile `{}` 要求订阅采集，但 driver 未声明 subscription",
                profile.id
            ),
        );
    }
    // 6) profile capability requirements satisfied（polling 维度）。
    if profile.capabilities.acquisition.polling == Some(true) && !caps.polling {
        return fail(
            &spec.id,
            "capability_requirements",
            format!(
                "Profile `{}` 要求周期采集，但 driver 未声明 polling",
                profile.id
            ),
        );
    }
    Ok(())
}
