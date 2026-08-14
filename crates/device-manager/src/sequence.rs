//! 按设备分配的单调递增序列号（§31.3 Delivery / Ordering / Deduplication）。
//!
//! # 语义
//!
//! `Observation.sequence` 在同一 `device_id` + 同一 Collector 会话内单调递增，
//! 跨多个采集组（不同 `interval_ms`）与多批次共享同一个序列源，保证下游
//! 去重与排序可靠；新 Collector 会话开始时由上层调用 [`SequenceAllocator::reset`]
//! 重置，使序列从 0 重新开始（会话 ID 已保证跨会话去重，§31.3）。

use std::collections::HashMap;

use observation_model::DeviceId;

/// 按设备分配序列号的单一状态组件。
///
/// 所有映射入口（`map_results` / `map_failure`）必须使用同一实例，
/// 避免多采集组产生重复/非单调的 `sequence`。
#[derive(Debug, Default)]
pub struct SequenceAllocator {
    next: HashMap<DeviceId, u64>,
}

impl SequenceAllocator {
    /// 空分配器（所有设备从 0 开始）。
    pub fn new() -> Self {
        Self::default()
    }

    /// 为设备分配 `count` 个连续序列号，返回本批起始序列号。
    ///
    /// 分配后该设备的下一批次将从 `start + count` 继续，保证跨批次单调。
    pub fn allocate(&mut self, device_id: &str, count: usize) -> u64 {
        let entry = self.next.entry(device_id.to_owned()).or_insert(0);
        let start = *entry;
        *entry += count as u64;
        start
    }

    /// 当前设备下一个可用的序列号。
    pub fn current(&self, device_id: &str) -> u64 {
        self.next.get(device_id).copied().unwrap_or(0)
    }

    /// 重置设备的序列（新 Collector 会话开始，§31.3）。
    pub fn reset(&mut self, device_id: &str) {
        self.next.insert(device_id.to_owned(), 0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocates_monotonic_sequences_per_device() {
        let mut allocator = SequenceAllocator::new();
        // 两个设备互不影响。
        assert_eq!(allocator.allocate("a", 2), 0);
        assert_eq!(allocator.allocate("a", 1), 2);
        assert_eq!(allocator.allocate("b", 3), 0);
        // 跨批次单调（a 的下一批从 3 开始）。
        assert_eq!(allocator.allocate("a", 1), 3);
        assert_eq!(allocator.current("a"), 4);
    }

    #[test]
    fn empty_batch_does_not_advance() {
        let mut allocator = SequenceAllocator::new();
        assert_eq!(allocator.allocate("a", 0), 0);
        assert_eq!(allocator.allocate("a", 1), 0);
    }

    #[test]
    fn reset_restarts_sequence() {
        let mut allocator = SequenceAllocator::new();
        allocator.allocate("a", 5);
        allocator.reset("a");
        assert_eq!(allocator.allocate("a", 1), 0);
    }
}
