// Copyright (c) 2026 The NORA Authors
// SPDX-License-Identifier: MIT

//! Bounded Maven/npm proxy-cache cleanup configuration.

use serde::{Deserialize, Serialize};
use std::env;

const DEFAULT_INTERVAL_SECS: u64 = 86_400;
const DEFAULT_MIN_CACHE_AGE_SECS: u64 = 30 * 86_400;
const DEFAULT_MIN_IDLE_SECS: u64 = 90 * 86_400;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProxyCacheCleanupConfig {
    /// Enable the background proxy-cache cleanup scheduler.
    #[serde(default)]
    pub enabled: bool,
    /// Report eligible payloads without deleting them. Access tracking remains
    /// active and may update hidden `.nora-proxy-access` markers.
    #[serde(default)]
    pub dry_run: bool,
    /// Delay between completed cleanup runs.
    #[serde(default = "default_interval_secs")]
    pub interval_secs: u64,
    /// A cache payload must be at least this old before it can be deleted.
    #[serde(default = "default_min_cache_age_secs")]
    pub min_cache_age_secs: u64,
    /// The persisted last access must be at least this old before deletion.
    #[serde(default = "default_min_idle_secs")]
    pub min_idle_secs: u64,
}

const fn default_interval_secs() -> u64 {
    DEFAULT_INTERVAL_SECS
}

const fn default_min_cache_age_secs() -> u64 {
    DEFAULT_MIN_CACHE_AGE_SECS
}

const fn default_min_idle_secs() -> u64 {
    DEFAULT_MIN_IDLE_SECS
}

impl Default for ProxyCacheCleanupConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            dry_run: false,
            interval_secs: DEFAULT_INTERVAL_SECS,
            min_cache_age_secs: DEFAULT_MIN_CACHE_AGE_SECS,
            min_idle_secs: DEFAULT_MIN_IDLE_SECS,
        }
    }
}

impl ProxyCacheCleanupConfig {
    pub(super) fn apply_env_overrides(&mut self) -> Result<(), String> {
        if let Ok(value) = env::var("NORA_PROXY_CACHE_CLEANUP_ENABLED") {
            self.enabled = parse_bool("NORA_PROXY_CACHE_CLEANUP_ENABLED", &value)?;
        }
        if let Ok(value) = env::var("NORA_PROXY_CACHE_CLEANUP_DRY_RUN") {
            self.dry_run = parse_bool("NORA_PROXY_CACHE_CLEANUP_DRY_RUN", &value)?;
        }
        if let Ok(value) = env::var("NORA_PROXY_CACHE_CLEANUP_INTERVAL_SECS") {
            self.interval_secs = parse_u64("NORA_PROXY_CACHE_CLEANUP_INTERVAL_SECS", &value)?;
        }
        if let Ok(value) = env::var("NORA_PROXY_CACHE_CLEANUP_MIN_CACHE_AGE_SECS") {
            self.min_cache_age_secs =
                parse_u64("NORA_PROXY_CACHE_CLEANUP_MIN_CACHE_AGE_SECS", &value)?;
        }
        if let Ok(value) = env::var("NORA_PROXY_CACHE_CLEANUP_MIN_IDLE_SECS") {
            self.min_idle_secs = parse_u64("NORA_PROXY_CACHE_CLEANUP_MIN_IDLE_SECS", &value)?;
        }
        Ok(())
    }
}

fn parse_bool(name: &str, value: &str) -> Result<bool, String> {
    match value.to_ascii_lowercase().as_str() {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        _ => Err(format!(
            "{name}={value:?} is invalid; expected true, false, 1 or 0"
        )),
    }
}

fn parse_u64(name: &str, value: &str) -> Result<u64, String> {
    value
        .parse()
        .map_err(|error| format!("{name}={value:?} is invalid: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_disabled_and_conservative() {
        let config = ProxyCacheCleanupConfig::default();
        assert!(!config.enabled);
        assert!(!config.dry_run);
        assert_eq!(config.interval_secs, 86_400);
        assert_eq!(config.min_cache_age_secs, 30 * 86_400);
        assert_eq!(config.min_idle_secs, 90 * 86_400);
    }

    #[test]
    fn partial_toml_keeps_policy_defaults() {
        let config: ProxyCacheCleanupConfig = toml::from_str("enabled = true").unwrap();
        assert!(config.enabled);
        assert_eq!(config.interval_secs, 86_400);
        assert_eq!(config.min_cache_age_secs, 30 * 86_400);
        assert_eq!(config.min_idle_secs, 90 * 86_400);
    }

    #[test]
    fn unknown_toml_field_is_fatal_for_destructive_policy() {
        let error = toml::from_str::<ProxyCacheCleanupConfig>("enabled = true\ndry_rnu = true")
            .unwrap_err();
        assert!(error.to_string().contains("unknown field `dry_rnu`"));
    }

    #[test]
    fn strict_boolean_parser_never_turns_a_typo_into_apply_mode() {
        assert!(parse_bool("TEST", "true").unwrap());
        assert!(!parse_bool("TEST", "0").unwrap());
        assert!(parse_bool("TEST", "treu").is_err());
    }

    #[test]
    fn environment_overrides_are_strict_and_take_precedence() {
        let _lock = super::super::ENV_MUTEX.lock().unwrap();
        std::env::set_var("NORA_PROXY_CACHE_CLEANUP_ENABLED", "1");
        std::env::set_var("NORA_PROXY_CACHE_CLEANUP_DRY_RUN", "true");
        std::env::set_var("NORA_PROXY_CACHE_CLEANUP_INTERVAL_SECS", "123");
        let mut config = ProxyCacheCleanupConfig::default();
        let result = config.apply_env_overrides();
        std::env::remove_var("NORA_PROXY_CACHE_CLEANUP_ENABLED");
        std::env::remove_var("NORA_PROXY_CACHE_CLEANUP_DRY_RUN");
        std::env::remove_var("NORA_PROXY_CACHE_CLEANUP_INTERVAL_SECS");
        result.unwrap();
        assert!(config.enabled);
        assert!(config.dry_run);
        assert_eq!(config.interval_secs, 123);
    }

    #[test]
    fn malformed_destructive_environment_override_fails_closed() {
        let _lock = super::super::ENV_MUTEX.lock().unwrap();
        std::env::set_var("NORA_PROXY_CACHE_CLEANUP_DRY_RUN", "treu");
        let mut config = ProxyCacheCleanupConfig::default();
        let result = config.apply_env_overrides();
        std::env::remove_var("NORA_PROXY_CACHE_CLEANUP_DRY_RUN");
        assert!(result.unwrap_err().contains("expected true, false, 1 or 0"));
    }
}
