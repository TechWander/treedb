//! 独立性门（DESIGN.md §11 第 7 条 T4）。
//!
//! 主张：treedb 是一个独立的数据库项目 —— 引擎内核不携带任何具体客户端的业务
//! 语义，也不把客户端的依赖塞进默认构建图。
//!
//! 这条约束光靠评审守不住：`KeyRec` 那一路字段就是「为了方便把客户端的表照抄
//! 进来」长出来的（现在仍在，但已被 `postgres-compat` 隔离，替换计划见 §11 T2）。
//! 所以这里放两把能在 CI 里跑的门，将来谁往内核里塞客户端名字或塞依赖，测试
//! 直接红。
//!
//! 本文件故意不扫描 `tests/`，并且关键词是拼出来的 —— 否则这个门会把自己
//! 拦下来。

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// 要守的内核目录：协议、状态机、事件循环。
/// 过渡档（`pool`/`store`/`persist`/`schema`/`probe`）不在这里 —— 它本来就是
/// 客户端脚手架，靠 feature 隔离，允许提到客户端名字。
const KERNEL_DIRS: &[&str] = &["src/engine", "src/net"];

fn needle() -> String {
    let (a, b) = ("grov", "efs");
    format!("{a}{b}")
}

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn collect_rs(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_rs(&path, out);
        } else if path.extension().map(|x| x == "rs").unwrap_or(false) {
            out.push(path);
        }
    }
}

/// 门 1：内核目录里不允许出现客户端的名字。
///
/// 判的是「参照系」：注释里写「与 POSIX `O_CREAT|O_EXCL` 一致」是语义对齐，写
/// 「与某个客户端的 create 主路径一致」就把产品关系倒过来了 —— 引擎不是任何
/// 人的模块。
#[test]
fn kernel_has_no_client_names() {
    let needle = needle();
    let mut offenders = Vec::new();
    for rel in KERNEL_DIRS {
        let dir = root().join(rel);
        let mut files = Vec::new();
        collect_rs(&dir, &mut files);
        assert!(
            !files.is_empty(),
            "{rel} 一个 .rs 都没有，这条门等于没跑（目录被改名了？）"
        );
        for file in files {
            let text = std::fs::read_to_string(&file).unwrap();
            for (lineno, line) in text.lines().enumerate() {
                if line.to_lowercase().contains(&needle) {
                    offenders.push(format!("{}:{}", file.display(), lineno + 1));
                }
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "引擎内核里出现了客户端名字，把它们改成语义本身（POSIX/FUSE/关系库通用形状），\n\
         或者把这段代码搬出内核目录。违规位置：\n  {}",
        offenders.join("\n  ")
    );
}

/// 门 2：默认构建图的直接依赖只能是 `libc` + `thiserror`。
///
/// 这是「能被任何项目引用的干净底座」的可执行定义。加新依赖要么走 optional
/// + feature，要么就是一次需要被看见的架构决定 —— 让它在这里失败，而不是在
/// 某个下游的 `cargo tree` 里被发现。
#[test]
fn default_dependency_graph_is_engine_only() {
    let text = std::fs::read_to_string(root().join("Cargo.toml")).unwrap();
    let allowed: BTreeSet<&str> = ["libc", "thiserror"].into_iter().collect();

    let mut in_deps = false;
    let mut extra = Vec::new();
    let mut saw_deps_section = false;
    for raw in text.lines() {
        let line = raw.trim();
        if line.starts_with('[') {
            in_deps = line == "[dependencies]";
            saw_deps_section |= in_deps;
            continue;
        }
        if !in_deps || line.is_empty() || line.starts_with('#') {
            continue;
        }
        let name = line.split('=').next().unwrap_or("").trim();
        let name = name.trim_matches(|c| c == '"' || c == '\'');
        if name.is_empty() {
            continue;
        }
        // optional 依赖不进默认图，由 feature 拉起，因此不算违规。
        let optional = line.replace(' ', "").contains("optional=true");
        if !optional && !allowed.contains(name) {
            extra.push(name.to_string());
        }
    }
    assert!(
        saw_deps_section,
        "Cargo.toml 里没有 [dependencies] 段？这条门没跑"
    );
    assert!(
        extra.is_empty(),
        "默认构建里出现了非引擎依赖：{extra:?}。把它们标 `optional = true` 并挂到 \
         feature 上（像 `postgres-compat` 那样），否则每个下游都被迫编它们。",
    );
}

/// 门 3：`postgres-compat` 必须是**默认关**的。
///
/// 门 2 只在 default feature 不含过渡档时才有意义；把两条绑在一起，免得有人
/// 图省事直接 `default = ["postgres-compat"]` 然后把门 2 变绿又变空话。
#[test]
fn compat_feature_is_not_default() {
    let text = std::fs::read_to_string(root().join("Cargo.toml")).unwrap();
    let mut in_features = false;
    let mut default_line: Option<String> = None;
    let mut has_compat = false;
    for raw in text.lines() {
        let line = raw.trim();
        if line.starts_with('[') {
            in_features = line == "[features]";
            continue;
        }
        if !in_features || line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.starts_with("default") {
            default_line = Some(line.to_string());
        }
        if line.starts_with("postgres-compat") {
            has_compat = true;
        }
    }
    assert!(
        has_compat,
        "`postgres-compat` feature 不见了：过渡档是否已从本仓移除？\n\
                         若确实移除，请连同这三把门一起改写，别留半套。"
    );
    if let Some(d) = default_line {
        assert!(
            !d.contains("postgres-compat"),
            "`default` 里不该有 postgres-compat（{d}）：默认档必须是纯引擎。"
        );
    }
}
