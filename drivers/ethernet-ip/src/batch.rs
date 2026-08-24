//! 批量读写规划：Multi-Service 子服务打包。
//!
//! # 与 S7/modbus 的结构差异
//!
//! - **读侧无跳洞/区间合并概念**——每个标签是独立子服务，"合并" =
//!   多个 Read Tag 子服务打包进同一 Multi 包（读取规划在 lib 层内联，
//!   本模块提供分块与写侧两段式规划）；
//! - **写侧无精确相邻约束**——各 Write Tag 子服务独立寻址，不存在
//!   覆盖未请求地址的风险；位项隔离概念同样消失（CIP BOOL 是 1 字节
//!   非位号）；
//! - 分块双上限均为**静态配置**：`max_services_per_multi`（项数）与
//!   `max_bytes_per_multi`（CIP 请求消息总字节）——EN/IP 无 PDU 协商，
//!   规划不依赖连接（与 S7 先握手后规划相反）。

use crate::address;
use crate::cip::{self, sub_request_overhead};
use crate::error::EtherIpError;

/// 写批量请求项（ABI 层转换产物）。
#[derive(Debug)]
pub struct WriteRequest {
    /// 上层关联 ID。
    pub id: u64,
    /// 标签路径。
    pub address: String,
    /// 写入值（已由 ABI Tag 解码）。
    pub value: observation_model::RawValue,
}

/// 类型发现缓存未命中项：需要先 Read Tag 探明类型码。
#[derive(Debug)]
pub struct TypeDiscovery {
    pub id: u64,
    pub tag: String,
    pub value: observation_model::RawValue,
}

/// 已知类型的写项：可直接编码为 Write Tag 子服务。
#[derive(Debug)]
pub struct ReadyWrite {
    pub id: u64,
    pub tag: String,
    pub type_code: u16,
    pub payload: Vec<u8>,
}

/// 写规划第一步：按类型缓存分流为「需发现」与「可直写」两组。
///
/// 缓存命中但编码失败（值域越界等）在此预填逐项失败，不发出必然失败
/// 的请求（镜像 S7 规划期剔除）。发现组同样按 canonical 升序。
///
/// # Errors
///
/// 地址解析失败返回 `invalid_address`（整体失败）。
pub fn plan_write_stage1(
    items: &[WriteRequest],
    type_cache: &std::collections::HashMap<String, u16>,
) -> Result<Stage1Split, EtherIpError> {
    let mut discovery = Vec::new();
    let mut ready = Vec::new();
    let mut failed = Vec::new();
    for req in items {
        let tag = address::parse(&req.address)
            .map_err(|e| EtherIpError::invalid_address(format!("{}: {e}", req.address)))?
            .raw;
        match type_cache.get(&tag) {
            Some(&type_code) => match crate::encode::encode_write(type_code, &req.value) {
                Ok(payload) => ready.push(ReadyWrite {
                    id: req.id,
                    tag,
                    type_code,
                    payload,
                }),
                Err(e) => failed.push((req.id, e)),
            },
            None => discovery.push(TypeDiscovery {
                id: req.id,
                tag,
                value: req.value.clone(),
            }),
        }
    }
    Ok((discovery, ready, failed))
}

/// 阶段 1 分流结果：`(需发现, 可直写, 预填失败)`。
type Stage1Split = (
    Vec<TypeDiscovery>,
    Vec<ReadyWrite>,
    Vec<(u64, EtherIpError)>,
);

/// 把就绪写项编码为 Write Tag 子请求并按双上限分块。
#[must_use]
pub fn plan_write_stage2(
    ready: &[ReadyWrite],
    max_services_per_multi: usize,
    max_bytes_per_multi: usize,
) -> Vec<Vec<Vec<u8>>> {
    let sub_requests: Vec<Vec<u8>> = ready
        .iter()
        .map(|w| cip::build_write_tag(&w.tag, w.type_code, &w.payload))
        .collect();
    chunk_multi(&sub_requests, max_services_per_multi, max_bytes_per_multi)
}

/// 按 `(项数, CIP 字节)` 双上限把子请求序列切分为 Multi 包（读/写共用）。
///
/// 字节预算含 Multi 封装固定开销（service+路径+计数域 ≈ 10B）与每条
/// 子请求的偏移表项。单项自身超预算时不强拆（保 item 完整），执行期
/// 由设备以错误应答暴露——与 modbus/S7 保完整策略一致。
pub fn chunk_multi(
    sub_requests: &[Vec<u8>],
    max_services: usize,
    max_bytes: usize,
) -> Vec<Vec<Vec<u8>>> {
    const MULTI_FIXED_OVERHEAD: usize = 10; // service(1)+path-words(1)+CM_PATH(4)+count(2)+余量
    let mut chunks: Vec<Vec<Vec<u8>>> = Vec::new();
    let mut current: Vec<Vec<u8>> = Vec::new();
    let mut current_bytes = MULTI_FIXED_OVERHEAD;
    for sub in sub_requests {
        let count = current.len() + 1;
        let bytes = current_bytes + sub.len() + sub_request_overhead();
        if !current.is_empty() && (count > max_services || bytes > max_bytes) {
            chunks.push(std::mem::take(&mut current));
            current_bytes = MULTI_FIXED_OVERHEAD;
        }
        current_bytes += sub.len() + sub_request_overhead();
        current.push(sub.clone());
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}
