//! 读取项生成与按采集周期分组（§22 Poll Scheduler、§100 Profile 与采集配置下发）。
//!
//! # 生成规则
//!
//! - 遍历 Profile 中 `readable == true` 的属性（§37 `ProfileProperty`）；
//! - 按属性声明顺序分配全局递增的 `item_id`（确定性：同一 Profile 每次生成
//!   结果一致，`RawReadResult.item_id` 可稳定回指属性）；
//! - 分组间隔取 `default_interval_ms`，属性未声明时归入调用方提供的
//!   默认组（§100 采集配置）；同一设备不同组的读取项互不重叠。

use std::collections::BTreeMap;
use std::sync::Arc;

use driver_sdk::DriverReadItem;
use observation_model::PropertyPath;
use profile_engine::{DeviceProfile, ProfileProperty};

/// 单个读取项（§22 Tag）。
#[derive(Debug, Clone, PartialEq)]
pub struct ReadItem {
    /// 设备内全局唯一 ID，对应 [`DriverReadItem`] 的 `id` 与
    /// `RawReadResult.item_id`。
    pub item_id: u64,
    /// 语义属性路径（§6.1），映射回 `ProfileProperty.path`。
    pub path: PropertyPath,
    /// 所属采集分组的间隔（毫秒）。
    pub interval_ms: u64,
    /// 发送给 Driver 的读取项（地址为 Driver 私有不透明数据，§10）。
    pub driver_item: DriverReadItem,
    /// 属性定义（缩放/单位/类型映射，§37.1 由 Profile 负责）。
    pub property: Arc<ProfileProperty>,
}

/// 同一采集周期的一组读取项（§22 Group）。
#[derive(Debug, Clone, PartialEq)]
pub struct ReadGroup {
    /// 采集间隔（毫秒），对应 `PollTarget.interval_ms`。
    pub interval_ms: u64,
    /// 本组读取项（按声明顺序）。
    pub read_items: Vec<ReadItem>,
    /// 发送给 Driver 的原始项（`DriverReadItem` 切片）。
    pub driver_items: Vec<DriverReadItem>,
}

/// 生成读取项：Profile 中所有可读属性 → 确定性 `item_id` 序列。
///
/// `default_interval_ms` 为属性未声明 `default_interval_ms` 时的分组间隔；
/// 必须非零（`0` 时分组校验会拒绝，见 [`group_read_items`]）。
pub fn generate_read_items(profile: &DeviceProfile, default_interval_ms: u64) -> Vec<ReadItem> {
    profile
        .properties
        .iter()
        .filter(|property| property.readable)
        .enumerate()
        .map(|(index, property)| ReadItem {
            item_id: index as u64,
            path: property.path.clone(),
            interval_ms: property.default_interval_ms.unwrap_or(default_interval_ms),
            driver_item: DriverReadItem {
                id: index as u64,
                address: property.driver_address.clone(),
                expected_type: Some(property.raw_type.clone()),
            },
            property: Arc::new(property.clone()),
        })
        .collect()
}

/// 按 `interval_ms` 分组（§22 Group），组间顺序按间隔升序。
///
/// # Panics
///
/// 读取项 `interval_ms` 为 0 时 panic（上层应在配置校验中拒绝）。
pub fn group_read_items(items: Vec<ReadItem>) -> Vec<ReadGroup> {
    let mut by_interval: BTreeMap<u64, Vec<ReadItem>> = BTreeMap::new();
    for item in items {
        assert!(
            item.interval_ms > 0,
            "读取项 `{}` 的采集间隔必须大于 0",
            item.path
        );
        by_interval.entry(item.interval_ms).or_default().push(item);
    }
    by_interval
        .into_iter()
        .map(|(interval_ms, read_items)| ReadGroup {
            interval_ms,
            driver_items: read_items
                .iter()
                .map(|item| item.driver_item.clone())
                .collect(),
            read_items,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use observation_model::{DataType, DomainKind};
    use profile_engine::WriteRounding;

    use super::*;

    fn sample_property(
        path: &str,
        address: &str,
        raw_type: DataType,
        readable: bool,
        interval: Option<u64>,
    ) -> ProfileProperty {
        ProfileProperty {
            path: path.to_owned(),
            driver_address: address.to_owned(),
            raw_type,
            value_type: DataType::F64,
            unit: None,
            scale: 1.0,
            offset: 0.0,
            write_rounding: WriteRounding::Nearest,
            readable,
            writable: true,
            default_interval_ms: interval,
            min: None,
            max: None,
        }
    }

    fn sample_profile() -> DeviceProfile {
        DeviceProfile {
            id: "test-profile".to_owned(),
            vendor: "Test".to_owned(),
            family: "T".to_owned(),
            models: vec!["T1".to_owned()],
            domain: DomainKind::Drive,
            driver_id: "modbus-tcp".to_owned(),
            properties: vec![
                sample_property("drive.a", "1!40001", DataType::U16, true, Some(100)),
                sample_property("drive.b", "1!40002", DataType::U16, true, Some(100)),
                sample_property("drive.c", "1!40003", DataType::U16, true, None),
                sample_property(
                    "drive.write_only",
                    "1!40004",
                    DataType::U16,
                    false,
                    Some(50),
                ),
            ],
            commands: vec![],
            capabilities: profile_engine::ProfileCapabilities {
                supported_properties: vec![],
                supported_commands: vec![],
                acquisition: Default::default(),
                limits: Default::default(),
            },
        }
    }

    #[test]
    fn generates_readable_properties_with_deterministic_ids() {
        let items = generate_read_items(&sample_profile(), 1000);
        assert_eq!(items.len(), 3);
        assert_eq!(
            items.iter().map(|i| i.path.as_str()).collect::<Vec<_>>(),
            vec!["drive.a", "drive.b", "drive.c"]
        );
        assert_eq!(items[0].item_id, 0);
        assert_eq!(items[2].item_id, 2);
        // 地址为 Driver 私有不透明数据，原样透传。
        assert_eq!(items[0].driver_item.address, "1!40001");
        assert_eq!(items[0].driver_item.expected_type, Some(DataType::U16));
    }

    #[test]
    fn groups_by_declared_interval_and_default() {
        let items = generate_read_items(&sample_profile(), 1000);
        let groups = group_read_items(items);
        assert_eq!(groups.len(), 2);
        // 间隔升序：100ms 组（a、b），1000ms 默认组（c）。
        assert_eq!(groups[0].interval_ms, 100);
        assert_eq!(groups[0].read_items.len(), 2);
        assert_eq!(groups[0].driver_items.len(), 2);
        assert_eq!(groups[1].interval_ms, 1000);
        assert_eq!(groups[1].read_items.len(), 1);
        // 不可读属性不进入任何组。
        assert!(
            groups
                .iter()
                .flat_map(|g| &g.read_items)
                .all(|i| i.path != "drive.write_only")
        );
    }

    #[test]
    fn empty_profile_yields_no_items() {
        let mut profile = sample_profile();
        profile.properties.clear();
        assert!(generate_read_items(&profile, 1000).is_empty());
    }
}
