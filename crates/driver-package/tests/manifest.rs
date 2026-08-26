//! Manifest v2 解析与静态校验（§7 规则逐条覆盖）。

use driver_package::{
    AbiSpec, ArtifactSpec, DriverManifestV2, ExecutionModel, Isolation, ManifestError, PackageKind,
    RuntimeSpec, SCHEMA_VERSION_V2,
};

fn valid_manifest_json() -> String {
    format!(
        r#"{{
  "schema_version": "2.0",
  "id": "modbus-tcp",
  "name": "Modbus TCP",
  "version": "0.1.0",
  "abi": {{ "major": 1, "minor": 0 }},
  "artifacts": {{
    "windows-x86_64": {{ "path": "driver_modbus.dll", "sha256": "{hash}" }}
  }},
  "runtime": {{
    "kind": "native",
    "execution_model": "blocking_bounded",
    "minimum_isolation": "per_driver",
    "default_isolation": "per_driver"
  }}
}}"#,
        hash = "a".repeat(64)
    )
}

#[test]
fn parse_valid_manifest() {
    let m = DriverManifestV2::parse(&valid_manifest_json()).expect("合法 manifest 应通过");
    assert_eq!(m.schema_version, SCHEMA_VERSION_V2);
    assert_eq!(m.id, "modbus-tcp");
    assert_eq!(m.runtime.execution_model, ExecutionModel::BlockingBounded);
    let artifact = m.artifact_for("windows-x86_64").expect("平台存在");
    assert_eq!(artifact.path, "driver_modbus.dll");
    assert!(
        m.artifact_for("linux-x86_64").is_none(),
        "未声明平台应返回 None"
    );
}

#[test]
fn rejects_missing_or_wrong_schema_version() {
    // 缺 schema_version：serde 必填字段缺失 → 解析失败。
    let no_version = valid_manifest_json().replace(r#""schema_version": "2.0","#, "");
    assert!(DriverManifestV2::parse(&no_version).is_err());

    // 错误版本 → UnsupportedSchema（§7.1：不静默接受非 2.0 语义）。
    let wrong = valid_manifest_json().replace(r#""2.0""#, r#""1.0""#);
    match DriverManifestV2::parse(&wrong) {
        Err(ManifestError::UnsupportedSchema(v)) => assert_eq!(v, "1.0"),
        other => panic!("应为 UnsupportedSchema，实际 {other:?}"),
    }
}

#[test]
fn rejects_path_escape() {
    // （JSON 文本中反斜杠须双写；被替换串 "driver_modbus.dll" 无转义歧义。）
    for bad in [
        "../outside/driver.dll",
        "/abs/driver.dll",
        "a/../../b.dll",
        "..\\\\outside.dll",
        "C:\\\\win\\\\driver.dll",
    ] {
        let json = valid_manifest_json().replace("driver_modbus.dll", bad);
        match DriverManifestV2::parse(&json) {
            Err(ManifestError::InvalidField {
                field: "artifacts.path",
                ..
            }) => {}
            other => panic!("路径 {bad:?} 应被拒绝，实际 {other:?}"),
        }
    }
}

#[test]
fn rejects_bad_sha256_format() {
    // 大写不允许（规范固定小写十六进制）。
    let upper = valid_manifest_json().replace(&"a".repeat(64), &"A".repeat(64));
    assert!(matches!(
        DriverManifestV2::parse(&upper),
        Err(ManifestError::InvalidField {
            field: "sha256",
            ..
        })
    ));
    // 长度错误。
    let short = valid_manifest_json().replace(&"a".repeat(64), &"a".repeat(63));
    assert!(matches!(
        DriverManifestV2::parse(&short),
        Err(ManifestError::InvalidField {
            field: "sha256",
            ..
        })
    ));
}

#[test]
fn default_isolation_may_not_be_weaker_than_minimum() {
    let json = r#"{
        "schema_version": "2.0",
        "id": "vendor-sdk",
        "name": "Vendor SDK",
        "version": "1.0.0",
        "abi": { "major": 1, "minor": 0 },
        "artifacts": { "windows-x86_64": { "path": "v.dll" } },
        "runtime": {
            "kind": "native",
            "execution_model": "blocking_uninterruptible",
            "minimum_isolation": "per_device",
            "default_isolation": "per_driver"
        }
    }"#;
    assert!(matches!(
        DriverManifestV2::parse(json),
        Err(ManifestError::InvalidField {
            field: "runtime.default_isolation",
            ..
        })
    ));
}

#[test]
fn isolation_ordering_strictness() {
    // Isolation 的 Ord 方向必须与"更严格"一致（§7：部署只允许相同或更严格）。
    assert!(Isolation::Shared < Isolation::PerDriver);
    assert!(Isolation::PerDriver < Isolation::PerDevice);
}

#[test]
fn empty_artifacts_and_empty_id_rejected() {
    let base = r#"{
        "schema_version": "2.0",
        "id": "ID",
        "name": "n",
        "version": "1.0.0",
        "abi": { "major": 1, "minor": 0 },
        "artifacts": {},
        "runtime": {
            "kind": "native",
            "execution_model": "blocking_bounded",
            "minimum_isolation": "shared",
            "default_isolation": "shared"
        }
    }"#;
    assert!(matches!(
        DriverManifestV2::parse(base),
        Err(ManifestError::InvalidField {
            field: "artifacts",
            ..
        })
    ));

    let empty_id = base.replace(r#""id": "ID""#, r#""id": """#);
    assert!(matches!(
        DriverManifestV2::parse(&empty_id),
        Err(ManifestError::InvalidField { field: "id", .. })
    ));

    let pathish_id = base.replace(r#""id": "ID""#, r#""id": "a/b""#);
    assert!(matches!(
        DriverManifestV2::parse(&pathish_id),
        Err(ManifestError::InvalidField { field: "id", .. })
    ));
}

#[test]
fn current_platform_matches_build_target() {
    // 三平台常量与打包脚本值域一致（§20）；本机编译目标必映射到其中之一
    // 或显式 "unknown"（由 scanner 报缺平台，不 panic）。
    let p = DriverManifestV2::current_platform();
    assert!(matches!(
        p,
        "windows-x86_64" | "linux-x86_64" | "linux-aarch64" | "unknown"
    ));
}

#[test]
fn execution_model_serde_shape_is_fixed() {
    // snake_case 形状固化（manifest JSON 与未来 ABI u32 映射的中间层）。
    assert_eq!(
        serde_json::to_string(&ExecutionModel::AsyncCancelable).unwrap(),
        r#""async_cancelable""#
    );
    assert_eq!(
        serde_json::to_string(&ExecutionModel::BlockingUninterruptible).unwrap(),
        r#""blocking_uninterruptible""#
    );
    assert_eq!(
        serde_json::to_string(&PackageKind::Native).unwrap(),
        r#""native""#
    );
}

#[test]
fn round_trip_preserves_all_fields() {
    let m = DriverManifestV2 {
        schema_version: SCHEMA_VERSION_V2.to_owned(),
        id: "s7comm".to_owned(),
        name: "Siemens S7comm".to_owned(),
        version: "0.3.0".to_owned(),
        abi: AbiSpec { major: 1, minor: 0 },
        artifacts: [
            (
                "linux-x86_64".to_owned(),
                ArtifactSpec {
                    path: "libdriver_s7comm.so".to_owned(),
                    sha256: Some("b".repeat(64)),
                },
            ),
            (
                "windows-x86_64".to_owned(),
                ArtifactSpec {
                    path: "driver_s7comm.dll".to_owned(),
                    sha256: None, // 开发态可缺省（§7 dev policy）
                },
            ),
        ]
        .into_iter()
        .collect(),
        runtime: RuntimeSpec {
            kind: PackageKind::Native,
            execution_model: ExecutionModel::BlockingBounded,
            minimum_isolation: Isolation::PerDriver,
            default_isolation: Isolation::PerDriver,
        },
        min_core_version: Some("0.3.0".to_owned()),
    };
    let json = serde_json::to_string_pretty(&m).expect("序列化失败");
    let back = DriverManifestV2::parse(&json).expect("往返后仍须合法");
    assert_eq!(m, back);
}
