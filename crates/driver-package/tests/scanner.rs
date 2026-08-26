//! Scanner 集成测试：目录发现 / duplicate id / hash 校验 / 路径逃逸（§6.3、§7）。

use std::path::PathBuf;

use driver_package::{ScanError, discover_package, scan_directories};

use sha2::{Digest, Sha256};
use tempfile::TempDir;

/// 构造一个最小合法 v2 manifest JSON。
fn manifest_json(id: &str, artifact_file: &str, sha256: Option<&str>) -> String {
    let hash_field = match sha256 {
        Some(h) => format!(r#""sha256": "{h}""#),
        None => String::new(),
    };
    let comma = if hash_field.is_empty() { "" } else { ", " };
    format!(
        r#"{{
  "schema_version": "2.0",
  "id": "{id}",
  "name": "Driver {id}",
  "version": "0.1.0",
  "abi": {{ "major": 1, "minor": 0 }},
  "artifacts": {{
    "{}": {{ "path": "{artifact_file}"{comma}{hash_field} }}
  }},
  "runtime": {{
    "kind": "native",
    "execution_model": "blocking_bounded",
    "minimum_isolation": "per_driver",
    "default_isolation": "per_driver"
  }}
}}"#,
        driver_package::DriverManifestV2::current_platform(),
    )
}

fn write_package(
    dir: &TempDir,
    name: &str,
    id: &str,
    artifact_bytes: &[u8],
    with_hash: bool,
) -> PathBuf {
    let root = dir.path().join(name);
    std::fs::create_dir_all(&root).expect("建目录失败");
    let artifact = format!("libdriver_{id}.bin");
    std::fs::write(root.join(&artifact), artifact_bytes).expect("写 artifact 失败");
    let sha256 = if with_hash {
        Some(format!("{:x}", Sha256::digest(artifact_bytes)))
    } else {
        None
    };
    std::fs::write(
        root.join("driver.json"),
        manifest_json(id, &artifact, sha256.as_deref()),
    )
    .expect("写 manifest 失败");
    root
}

#[test]
fn discovers_and_validates_single_package_with_hash() {
    let dir = TempDir::new().unwrap();
    let bytes = b"fake-artifact-bytes";
    let root = write_package(&dir, "modbus", "modbus-tcp", bytes, true);

    let d = discover_package(&root, None).expect("发现应成功");
    assert_eq!(d.id(), "modbus-tcp");
    assert_eq!(d.version(), "0.1.0");
    assert_eq!(
        d.artifact_sha256,
        format!("{:x}", Sha256::digest(bytes)),
        "实测 hash 应与内容一致"
    );
    // canonical 路径位于 root 内且文件存在。
    assert!(d.artifact_path.is_absolute());
    assert!(d.artifact_path.starts_with(root.canonicalize().unwrap()));
}

#[test]
fn dev_policy_manifest_without_sha256_still_discovers_and_records_hash() {
    // §7 dev policy：开发态可缺省 sha256 字段；scanner 记录实测值，
    // 发布打包必须回填后才是完整发布包。
    let dir = TempDir::new().unwrap();
    let root = write_package(&dir, "mc", "mitsubishi-mc", b"mc-bytes", false);
    let d = discover_package(&root, None).expect("无 hash 开发包应可通过");
    assert_eq!(
        d.artifact_sha256,
        format!("{:x}", Sha256::digest(b"mc-bytes"))
    );
}

#[test]
fn hash_mismatch_rejected() {
    let dir = TempDir::new().unwrap();
    let root = write_package(&dir, "s7", "s7comm", b"original", true);
    // 扫描后替换文件内容（模拟"扫描后被替换"，此处直接验证比对逻辑）。
    std::fs::write(root.join("libdriver_s7comm.bin"), b"tampered").unwrap();
    match discover_package(&root, None) {
        Err(ScanError::Artifact { reason, .. }) => {
            assert!(reason.contains("hash 不匹配"), "实际: {reason}");
        }
        other => panic!("应报 hash 不匹配，实际 {other:?}"),
    }
}

#[test]
fn missing_artifact_rejected() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().join("enip");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(
        root.join("driver.json"),
        manifest_json("ethernet-ip", "missing.dll", None),
    )
    .unwrap();
    match discover_package(&root, None) {
        Err(ScanError::Artifact { reason, .. }) => {
            assert!(reason.contains("不存在"), "实际: {reason}");
        }
        other => panic!("应报 artifact 缺失，实际 {other:?}"),
    }
}

#[test]
fn symlink_escape_rejected() {
    // §7：symlink 指向包外文件 → canonicalize 后不在 root 内 → 拒绝。
    // Windows 上创建 symlink 需要特权，CI/本地都可能失败——跳过而非误报。
    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;
        let dir = TempDir::new().unwrap();
        let outside = dir.path().join("outside.bin");
        std::fs::write(&outside, b"outside").unwrap();
        let root = dir.path().join("pkg");
        std::fs::create_dir_all(&root).unwrap();
        symlink(&outside, root.join("escape.bin")).expect("创建 symlink 失败");
        std::fs::write(
            root.join("driver.json"),
            manifest_json("evil", "escape.bin", None),
        )
        .unwrap();
        match discover_package(&root, None) {
            Err(ScanError::Artifact { reason, .. }) => {
                assert!(reason.contains("逃逸"), "实际: {reason}");
            }
            other => panic!("应拒绝包外 symlink artifact，实际 {other:?}"),
        }
    }
}

#[test]
fn scan_directories_finds_multiple_packages() {
    // §6.3 验收基础能力：一个扫描集合内发现多个不同 Driver 包。
    let dir = TempDir::new().unwrap();
    write_package(&dir, "a-modbus", "modbus-tcp", b"a", true);
    write_package(&dir, "b-s7", "s7comm", b"b", true);

    let found = scan_directories(&[dir.path().to_owned()]).expect("扫描应成功");
    let mut ids: Vec<&str> = found.iter().map(|d| d.id()).collect();
    ids.sort();
    assert_eq!(ids, vec!["modbus-tcp", "s7comm"]);
}

#[test]
fn duplicate_id_across_packages_rejected() {
    let dir = TempDir::new().unwrap();
    write_package(&dir, "one", "dup-id", b"1", true);
    write_package(&dir, "two", "dup-id", b"2", true);
    match scan_directories(&[dir.path().to_owned()]) {
        Err(ScanError::DuplicateId { id, .. }) => assert_eq!(id, "dup-id"),
        other => panic!("应报重复 id，实际 {other:?}"),
    }
}

#[test]
fn invalid_manifest_in_any_package_fails_whole_scan() {
    // MVP fail-fast（§29 精神）：不静默跳过损坏包。
    let dir = TempDir::new().unwrap();
    write_package(&dir, "good", "good-id", b"g", true);
    let bad = dir.path().join("bad");
    std::fs::create_dir_all(&bad).unwrap();
    std::fs::write(bad.join("driver.json"), "{ not json").unwrap();

    match scan_directories(&[dir.path().to_owned()]) {
        Err(ScanError::Manifest { path, .. }) => {
            assert_eq!(path, bad.join("driver.json"));
        }
        other => panic!("应报 manifest 非法，实际 {other:?}"),
    }
}

#[test]
fn directories_without_driver_json_are_ignored() {
    let dir = TempDir::new().unwrap();
    write_package(&dir, "pkg", "real-driver", b"x", true);
    // 无 driver.json 的子目录与散落文件都应被跳过。
    std::fs::create_dir_all(dir.path().join("empty-dir")).unwrap();
    std::fs::write(dir.path().join("loose.txt"), "not a package").unwrap();

    let found = scan_directories(&[dir.path().to_owned()]).expect("扫描应成功");
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].id(), "real-driver");
}
