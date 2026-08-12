// Copyright (c) 2026 The NORA Authors
// SPDX-License-Identifier: MIT

use serde::{Deserialize, Serialize};

fn default_path() -> String {
    "data/index/nora.redb".to_string()
}

fn default_reconcile_interval_secs() -> u64 {
    3600
}

/// Persistent derived Maven/npm index configuration.
///
/// S3 remains authoritative: deleting this file may increase warm-up time but
/// must never change artifact protocol semantics.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct IndexConfig {
    pub path: String,
    pub reconcile_interval_secs: u64,
}

impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            path: default_path(),
            reconcile_interval_secs: default_reconcile_interval_secs(),
        }
    }
}

impl IndexConfig {
    pub(super) fn apply_env_overrides(&mut self) {
        if let Ok(value) = std::env::var("NORA_INDEX_PATH") {
            self.path = value;
        }
        if let Ok(value) = std::env::var("NORA_INDEX_RECONCILE_INTERVAL_SECS") {
            super::parse_env_warn(
                "NORA_INDEX_RECONCILE_INTERVAL_SECS",
                &value,
                &mut self.reconcile_interval_secs,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::IndexConfig;

    #[test]
    fn defaults_are_persistent_and_hourly() {
        let config = IndexConfig::default();
        assert_eq!(config.path, "data/index/nora.redb");
        assert_eq!(config.reconcile_interval_secs, 3600);
    }

    #[test]
    fn unknown_fields_fail_closed() {
        let error = toml::from_str::<IndexConfig>("pth = 'typo.redb'").unwrap_err();
        assert!(error.to_string().contains("unknown field"));
    }

    #[test]
    fn environment_overrides_path_and_reconcile_interval() {
        let _lock = super::super::ENV_MUTEX.lock().unwrap();
        std::env::set_var("NORA_INDEX_PATH", "/var/lib/nora/custom.redb");
        std::env::set_var("NORA_INDEX_RECONCILE_INTERVAL_SECS", "900");
        let mut config = IndexConfig::default();
        config.apply_env_overrides();
        std::env::remove_var("NORA_INDEX_PATH");
        std::env::remove_var("NORA_INDEX_RECONCILE_INTERVAL_SECS");
        assert_eq!(config.path, "/var/lib/nora/custom.redb");
        assert_eq!(config.reconcile_interval_secs, 900);
    }
}
