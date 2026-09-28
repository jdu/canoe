//! Local, per-developer project registry: which `.hcl` files this
//! `canoe-tui` instance knows about and can push to a coordinator — a
//! small JSON file at an XDG-convention path, not anything the
//! coordinator itself is aware of (the coordinator's own live registry,
//! `crates/canoe-coordinator/src/projects.rs`, is a separate, in-memory
//! thing populated by pushes).

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalProject {
    pub name: String,
    pub hcl_path: PathBuf,
    pub is_dev: bool,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct ProjectsFile {
    #[serde(default)]
    projects: Vec<LocalProject>,
}

/// `$XDG_DATA_HOME/canoe-tui/projects.json`, falling back to
/// `~/.local/share/canoe-tui/projects.json` when `XDG_DATA_HOME` isn't
/// set — the standard XDG base-directory convention, without pulling in
/// a dedicated crate just for this one path.
pub fn default_path() -> anyhow::Result<PathBuf> {
    let base = if let Ok(xdg) = std::env::var("XDG_DATA_HOME") {
        PathBuf::from(xdg)
    } else {
        let home = std::env::var("HOME").map_err(|_| anyhow::anyhow!("neither XDG_DATA_HOME nor HOME is set"))?;
        PathBuf::from(home).join(".local").join("share")
    };
    Ok(base.join("canoe-tui").join("projects.json"))
}

/// Missing file is not an error — an empty registry, same as a fresh
/// install with nothing added yet.
pub fn load(path: &Path) -> anyhow::Result<Vec<LocalProject>> {
    if !path.is_file() {
        return Ok(Vec::new());
    }
    let raw = std::fs::read_to_string(path)?;
    let file: ProjectsFile = serde_json::from_str(&raw)?;
    Ok(file.projects)
}

pub fn save(path: &Path, projects: &[LocalProject]) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = ProjectsFile { projects: projects.to_vec() };
    std::fs::write(path, serde_json::to_string_pretty(&file)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_file_loads_as_empty() {
        let dir = std::env::temp_dir().join(format!("canoe-tui-test-{}", std::process::id()));
        let path = dir.join("does-not-exist.json");
        assert!(load(&path).unwrap().is_empty());
    }

    #[test]
    fn save_then_load_round_trips() {
        let dir = std::env::temp_dir().join(format!("canoe-tui-test-{}-{}", std::process::id(), line!()));
        let path = dir.join("projects.json");
        let projects = vec![LocalProject { name: "my-pipeline".to_string(), hcl_path: PathBuf::from("/tmp/x.hcl"), is_dev: true }];
        save(&path, &projects).unwrap();
        let loaded = load(&path).unwrap();
        assert_eq!(loaded, projects);
        std::fs::remove_dir_all(&dir).ok();
    }
}
