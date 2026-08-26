//! v1 → v2 离线迁移测试（§7.1 规则 2）。

use driver_package::{ExecutionModel, Isolation, MigrationError, migrate_v1_json};

const V1_JSON: &str = r#"{
    "id": "modbus-tcp",
    "name": "Modbus TCP",
    "version": "0.1.0",
    "entry": "forgelink_driver_entry_v1",
    "abi": { "major": 1, "minor": 0 },
    "platforms": ["windows-x86_64", "linux-x86_64", "linux-aarch64"]
}"#;

#[test]
fn migrates_v1_preserving_identity_and_platforms() {
    let v2 = migrate_v1_json(V1_JSON).expect("迁移应成功");
    assert_eq!(v2.schema_version, "2.0");
    assert_eq!(v2.id, "modbus-tcp");
    assert_eq!(v2.name, "Modbus TCP");
    assert_eq!(v2.version, "0.1.0");
    assert_eq!((v2.abi.major, v2.abi.minor), (1, 0));
    // 每个 v1 平台生成一个无 hash 占位（path 由打包流程校准回填）。
    assert_eq!(v2.artifacts.len(), 3);
    assert!(v2.artifacts["windows-x86_64"].sha256.is_none());
}

#[test]
fn migrated_runtime_defaults_are_conservative() {
    // 现有 ABI v1 Driver 全部是同步阻塞函数表：默认 blocking_bounded +
    // per_driver（§7：不得凭内部 Tokio 声明 async_cancelable）。
    let v2 = migrate_v1_json(V1_JSON).unwrap();
    assert_eq!(v2.runtime.execution_model, ExecutionModel::BlockingBounded);
    assert_eq!(v2.runtime.minimum_isolation, Isolation::PerDriver);
    assert_eq!(v2.runtime.default_isolation, Isolation::PerDriver);
}

#[test]
fn refuses_to_downgrade_v2_input() {
    let v2 = r#"{ "schema_version": "2.0", "id": "x" }"#;
    match migrate_v1_json(v2) {
        Err(MigrationError::AlreadyV2) => {}
        other => panic!("带 schema_version 的输入应拒绝迁移，实际 {other:?}"),
    }
}

#[test]
fn rejects_v1_without_platforms() {
    let no_platforms = r#"{
        "id": "x",
        "name": "X",
        "version": "1.0.0",
        "abi": { "major": 1, "minor": 0 },
        "platforms": []
    }"#;
    assert!(matches!(
        migrate_v1_json(no_platforms),
        Err(MigrationError::Invalid { .. })
    ));
}
