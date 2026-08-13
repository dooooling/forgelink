//! `DomainKind` → 标准路径前缀表（§41~§46、§47）。
//!
//! 前缀与 `DomainKind` 的 snake_case 序列化名一致（如 `Drive` → `drive`），
//! 并预留 `Custom` 域（未纳入标准枚举的自定义设备类别）。

use observation_model::DomainKind;

/// 返回 `DomainKind` 的标准路径前缀（不含尾部点）。
pub fn standard_prefix(domain: &DomainKind) -> &'static str {
    match domain {
        DomainKind::Plc => "plc",
        DomainKind::Cnc => "cnc",
        DomainKind::Robot => "robot",
        DomainKind::Drive => "drive",
        DomainKind::Servo => "servo",
        DomainKind::Meter => "meter",
        DomainKind::Sensor => "sensor",
        DomainKind::Instrument => "instrument",
        DomainKind::Machine => "machine",
        DomainKind::PowerDevice => "power_device",
        DomainKind::BuildingDevice => "building_device",
        // 自定义类别：统一归入 `custom` 前缀，由上层进一步区分。
        DomainKind::Custom(_) => "custom",
    }
}

/// 将语义路径的首段归一为标准前缀（供领域路径校验使用）。
pub(crate) fn first_segment(path: &str) -> &str {
    path.split('.').next().unwrap_or(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_matches_doc_examples() {
        assert_eq!(standard_prefix(&DomainKind::Drive), "drive");
        assert_eq!(standard_prefix(&DomainKind::Cnc), "cnc");
        assert_eq!(standard_prefix(&DomainKind::Plc), "plc");
        assert_eq!(standard_prefix(&DomainKind::PowerDevice), "power_device");
        assert_eq!(
            standard_prefix(&DomainKind::Custom("foo".to_owned())),
            "custom"
        );
    }

    #[test]
    fn first_segment_handles_edges() {
        assert_eq!(first_segment("drive.output.frequency"), "drive");
        assert_eq!(first_segment("drive"), "drive");
        assert_eq!(first_segment(""), "");
    }
}
