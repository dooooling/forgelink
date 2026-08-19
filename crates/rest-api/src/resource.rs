//! 资源树派生（§5 Resource 最小实现）。
//!
//! Collector 当前只加载 Profile 属性（§37），不维护独立资源声明；本
//! 模块按语义属性路径的层级结构派生资源树：
//!
//! ```text
//! drive.output.frequency  ──>  资源 drive ──> 资源 drive.output ──> 属性 frequency
//! ```
//!
//! - 资源路径为属性路径去掉末段（属性本身不构成资源）；
//! - `kind` 取资源路径首段（对应 Domain 标准前缀，§41~§47）；
//! - 每资源列出直接挂载的属性路径与子资源路径；
//! - 属性路径非法（空段）时按字面保留，不 panic。

use std::collections::{BTreeMap, BTreeSet};

use crate::models::ResourceView;

/// 从属性路径集合派生资源树（顶层资源列表，含嵌套 `children`）。
///
/// `paths` 为空时返回空列表。同一路径的重复属性只出现一次。
pub fn derive_resources(paths: impl IntoIterator<Item = impl AsRef<str>>) -> Vec<ResourceView> {
    // 资源路径 → 直接挂载的属性集合。
    let mut properties: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    // 父资源路径 → 子资源路径集合。
    let mut children: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut all: BTreeSet<String> = BTreeSet::new();

    for path in paths {
        let path = path.as_ref().trim();
        if path.is_empty() {
            continue;
        }
        let segments: Vec<&str> = path.split('.').collect();
        let last = segments.len() - 1;
        // 中间段构成资源链，末段是属性。
        for depth in 0..last {
            let resource: String = segments[..=depth].join(".");
            all.insert(resource.clone());
            if depth + 1 == last {
                properties
                    .entry(resource)
                    .or_default()
                    .insert(path.to_owned());
            }
        }
        for depth in 1..last {
            let parent: String = segments[..depth].join(".");
            let child: String = segments[..=depth].join(".");
            children.entry(parent).or_default().insert(child);
        }
    }

    all.iter()
        .map(|path| ResourceView {
            path: path.clone(),
            kind: path.split('.').next().unwrap_or(path).to_owned(),
            display_name: path.clone(),
            properties: properties
                .remove(path)
                .map(|s| s.into_iter().collect())
                .unwrap_or_default(),
            children: children
                .remove(path)
                .map(|s| s.into_iter().collect())
                .unwrap_or_default(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_resource_tree_from_property_paths() {
        let resources = derive_resources([
            "drive.output.frequency",
            "drive.output.current",
            "drive.run.status",
        ]);
        let by_path: BTreeMap<&str, &ResourceView> =
            resources.iter().map(|r| (r.path.as_str(), r)).collect();

        let drive = by_path["drive"];
        assert_eq!(drive.kind, "drive");
        assert!(drive.properties.is_empty(), "drive 无直接属性");
        assert_eq!(drive.children, vec!["drive.output", "drive.run"]);

        let output = by_path["drive.output"];
        assert_eq!(output.kind, "drive");
        assert_eq!(
            output.properties,
            vec!["drive.output.current", "drive.output.frequency"]
        );
        assert!(output.children.is_empty());

        let run = by_path["drive.run"];
        assert_eq!(run.properties, vec!["drive.run.status"]);
    }

    #[test]
    fn single_segment_property_yields_no_resource() {
        assert!(derive_resources(["status"]).is_empty());
    }

    #[test]
    fn empty_input_yields_empty_tree() {
        assert!(derive_resources([] as [&str; 0]).is_empty());
    }

    #[test]
    fn duplicate_properties_deduplicated() {
        let resources = derive_resources(["drive.a.x", "drive.a.x"]);
        let a = resources
            .iter()
            .find(|r| r.path == "drive.a")
            .expect("drive.a 存在");
        assert_eq!(a.properties, vec!["drive.a.x"]);
    }

    #[test]
    fn empty_segments_kept_literal_without_panic() {
        let resources = derive_resources(["drive..x"]);
        assert_eq!(resources.len(), 2, "空段按字面保留");
        let root = resources
            .iter()
            .find(|r| r.path == "drive")
            .expect("根资源");
        assert_eq!(root.children, vec!["drive."]);
    }
}
