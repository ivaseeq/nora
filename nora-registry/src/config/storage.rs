// Copyright (c) 2026 The NORA Authors
// SPDX-License-Identifier: MIT

//! Storage backend configuration.

use crate::secrets::ProtectedString;
use serde::{Deserialize, Serialize};
use std::env;
use std::time::Duration;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum StorageMode {
    #[default]
    Local,
    S3,
    Gcs,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    #[serde(default)]
    pub mode: StorageMode,
    #[serde(default = "default_storage_path")]
    pub path: String,
    #[serde(default = "default_s3_url")]
    pub s3_url: String,
    #[serde(default = "default_bucket")]
    pub bucket: String,
    /// S3 access key (optional, uses anonymous access if not set)
    #[serde(default, skip_serializing)]
    pub s3_access_key: Option<ProtectedString>,
    /// S3 secret key (optional, uses anonymous access if not set)
    #[serde(default, skip_serializing)]
    pub s3_secret_key: Option<ProtectedString>,
    /// S3 region (default: us-east-1)
    #[serde(default = "default_s3_region")]
    pub s3_region: String,
    /// Use virtual-hosted-style requests. Default: false (path-style, bucket appended
    /// to the endpoint path). When true, `s3_url` is used VERBATIM and must already
    /// include the bucket host, e.g. `https://<bucket>.oss-<region>.aliyuncs.com`.
    /// Some providers reject signed path-style requests entirely — e.g. Alibaba
    /// Cloud OSS answers them with 403 `SecondLevelDomainForbidden`.
    #[serde(default)]
    pub s3_virtual_hosted: bool,
    /// GCS: path to a service-account JSON key file. Unset = ambient
    /// credentials (GOOGLE_* env, then the instance metadata server — GKE
    /// Workload Identity / GCE service accounts need no key material).
    #[serde(default)]
    pub gcs_service_account_path: Option<String>,
    /// GCS: endpoint override for emulators (fake-gcs-server) or Private
    /// Google Access endpoints. Unset = https://storage.googleapis.com.
    /// Emulator-only caveat: an `http://` override also disables request
    /// signing — do not point this at a plaintext non-emulator endpoint.
    #[serde(default)]
    pub gcs_base_url: Option<String>,
    /// Overall timeout for one object-store request, including a streamed body.
    #[serde(default = "default_request_timeout_secs")]
    pub request_timeout_secs: u64,
    /// Deadline covering the initial attempt and transparent retries.
    #[serde(default = "default_retry_timeout_secs")]
    pub retry_timeout_secs: u64,
    /// Maximum transparent retries performed by `object_store`.
    #[serde(default = "default_max_retries")]
    pub max_retries: usize,
    /// Independent wall-time bound for the background reachability probe.
    #[serde(default = "default_health_probe_timeout_secs")]
    pub health_probe_timeout_secs: u64,
}

fn default_request_timeout_secs() -> u64 {
    3_600
}

fn default_retry_timeout_secs() -> u64 {
    7_200
}

fn default_max_retries() -> usize {
    10
}

fn default_health_probe_timeout_secs() -> u64 {
    10
}

pub(super) fn default_s3_region() -> String {
    "us-east-1".to_string()
}

pub(super) fn default_storage_path() -> String {
    "data/storage".to_string()
}

pub(super) fn default_s3_url() -> String {
    "http://127.0.0.1:9000".to_string()
}

pub(super) fn default_bucket() -> String {
    "registry".to_string()
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            mode: StorageMode::Local,
            path: default_storage_path(),
            s3_url: default_s3_url(),
            bucket: default_bucket(),
            s3_access_key: None,
            s3_secret_key: None,
            s3_region: default_s3_region(),
            s3_virtual_hosted: false,
            gcs_service_account_path: None,
            gcs_base_url: None,
            request_timeout_secs: default_request_timeout_secs(),
            retry_timeout_secs: default_retry_timeout_secs(),
            max_retries: default_max_retries(),
            health_probe_timeout_secs: default_health_probe_timeout_secs(),
        }
    }
}

impl StorageConfig {
    /// Apply environment variable overrides for storage config.
    ///
    /// Returns `Err` if `NORA_STORAGE_MODE` has an unrecognized value — fail-closed (#562).
    pub(super) fn apply_env_overrides(&mut self) -> Result<(), String> {
        if let Ok(val) = env::var("NORA_STORAGE_MODE") {
            self.mode = match val.to_lowercase().as_str() {
                "local" | "filesystem" => StorageMode::Local,
                "s3" => StorageMode::S3,
                "gcs" => StorageMode::Gcs,
                other => {
                    return Err(format!(
                        "NORA_STORAGE_MODE={:?} is invalid — valid values: local, s3, gcs",
                        other
                    ))
                }
            };
        }
        if let Ok(val) = env::var("NORA_STORAGE_PATH") {
            self.path = val;
        }
        if let Ok(val) = env::var("NORA_STORAGE_S3_URL") {
            self.s3_url = val;
        }
        if let Ok(val) = env::var("NORA_STORAGE_BUCKET") {
            self.bucket = val;
        }
        if let Ok(val) = env::var("NORA_STORAGE_GCS_SERVICE_ACCOUNT_PATH") {
            self.gcs_service_account_path = if val.is_empty() { None } else { Some(val) };
        }
        if let Ok(val) = env::var("NORA_STORAGE_GCS_BASE_URL") {
            self.gcs_base_url = if val.is_empty() { None } else { Some(val) };
        }
        if let Ok(val) = env::var("NORA_STORAGE_S3_ACCESS_KEY") {
            self.s3_access_key = if val.is_empty() {
                None
            } else {
                Some(ProtectedString::new(val))
            };
        }
        if let Ok(val) = env::var("NORA_STORAGE_S3_SECRET_KEY") {
            self.s3_secret_key = if val.is_empty() {
                None
            } else {
                Some(ProtectedString::new(val))
            };
        }
        if let Ok(val) = env::var("NORA_STORAGE_S3_REGION") {
            self.s3_region = val;
        }
        if let Ok(val) = env::var("NORA_STORAGE_S3_VIRTUAL_HOSTED") {
            self.s3_virtual_hosted = val.to_lowercase() == "true" || val == "1";
        }
        if let Ok(val) = env::var("NORA_STORAGE_REQUEST_TIMEOUT_SECS") {
            self.request_timeout_secs = parse_env("NORA_STORAGE_REQUEST_TIMEOUT_SECS", &val)?;
        }
        if let Ok(val) = env::var("NORA_STORAGE_RETRY_TIMEOUT_SECS") {
            self.retry_timeout_secs = parse_env("NORA_STORAGE_RETRY_TIMEOUT_SECS", &val)?;
        }
        if let Ok(val) = env::var("NORA_STORAGE_MAX_RETRIES") {
            self.max_retries = parse_env("NORA_STORAGE_MAX_RETRIES", &val)?;
        }
        if let Ok(val) = env::var("NORA_STORAGE_HEALTH_PROBE_TIMEOUT_SECS") {
            self.health_probe_timeout_secs =
                parse_env("NORA_STORAGE_HEALTH_PROBE_TIMEOUT_SECS", &val)?;
        }

        Ok(())
    }

    pub fn object_store_options(&self) -> crate::storage::ObjectStoreOptions {
        crate::storage::ObjectStoreOptions {
            request_timeout: Duration::from_secs(self.request_timeout_secs),
            retry_timeout: Duration::from_secs(self.retry_timeout_secs),
            max_retries: self.max_retries,
            health_probe_timeout: Duration::from_secs(self.health_probe_timeout_secs),
        }
    }
}

fn parse_env<T>(name: &str, value: &str) -> Result<T, String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    value
        .parse()
        .map_err(|error| format!("{name}={value:?} is invalid: {error}"))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_gcs_mode_parses_from_toml_and_env() {
        let _lock = super::super::ENV_MUTEX.lock().unwrap();
        let cfg: StorageConfig = toml::from_str("mode = \"gcs\"\nbucket = \"artifacts\"").unwrap();
        assert_eq!(cfg.mode, StorageMode::Gcs);
        assert_eq!(cfg.bucket, "artifacts");
        assert_eq!(cfg.gcs_service_account_path, None);
        assert_eq!(cfg.gcs_base_url, None);

        let mut cfg = StorageConfig::default();
        std::env::set_var("NORA_STORAGE_MODE", "gcs");
        std::env::set_var("NORA_STORAGE_GCS_SERVICE_ACCOUNT_PATH", "/sa.json");
        std::env::set_var("NORA_STORAGE_GCS_BASE_URL", "http://127.0.0.1:4443");
        let r = cfg.apply_env_overrides();
        std::env::remove_var("NORA_STORAGE_MODE");
        std::env::remove_var("NORA_STORAGE_GCS_SERVICE_ACCOUNT_PATH");
        std::env::remove_var("NORA_STORAGE_GCS_BASE_URL");
        r.unwrap();
        assert_eq!(cfg.mode, StorageMode::Gcs);
        assert_eq!(cfg.gcs_service_account_path.as_deref(), Some("/sa.json"));
        assert_eq!(cfg.gcs_base_url.as_deref(), Some("http://127.0.0.1:4443"));
    }

    #[test]
    fn test_unknown_mode_still_fails_closed() {
        let _lock = super::super::ENV_MUTEX.lock().unwrap();
        let mut cfg = StorageConfig::default();
        std::env::set_var("NORA_STORAGE_MODE", "azure");
        let r = cfg.apply_env_overrides();
        std::env::remove_var("NORA_STORAGE_MODE");
        let err = r.unwrap_err();
        assert!(err.contains("local, s3, gcs"), "{err}");
    }

    #[test]
    fn object_store_timeout_defaults_are_transfer_safe() {
        let cfg: StorageConfig = toml::from_str("mode = \"s3\"").unwrap();
        let options = cfg.object_store_options();
        assert_eq!(options.request_timeout, Duration::from_secs(3_600));
        assert_eq!(options.retry_timeout, Duration::from_secs(7_200));
        assert_eq!(options.max_retries, 10);
        assert_eq!(options.health_probe_timeout, Duration::from_secs(10));
    }

    #[test]
    fn object_store_timeouts_are_configurable() {
        let cfg: StorageConfig = toml::from_str(
            r#"
                mode = "gcs"
                request_timeout_secs = 120
                retry_timeout_secs = 300
                max_retries = 4
                health_probe_timeout_secs = 7
            "#,
        )
        .unwrap();
        let options = cfg.object_store_options();
        assert_eq!(options.request_timeout, Duration::from_secs(120));
        assert_eq!(options.retry_timeout, Duration::from_secs(300));
        assert_eq!(options.max_retries, 4);
        assert_eq!(options.health_probe_timeout, Duration::from_secs(7));
    }

    #[test]
    fn malformed_timeout_override_fails_closed() {
        assert!(parse_env::<u64>("NORA_STORAGE_REQUEST_TIMEOUT_SECS", "slow").is_err());
    }
}
