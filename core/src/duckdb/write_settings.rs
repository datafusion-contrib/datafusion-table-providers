/*
Copyright 2024-2025 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

use std::collections::HashMap;

/// Configuration settings for DuckDB write operations
#[derive(Debug, Clone)]
pub struct DuckDBWriteSettings {
    /// Whether to execute ANALYZE statements after data refresh operations
    /// to update table statistics for query optimization
    pub recompute_statistics_on_refresh: bool,
}

impl Default for DuckDBWriteSettings {
    fn default() -> Self {
        Self {
            recompute_statistics_on_refresh: true, // Enabled by default for better query performance
        }
    }
}

impl DuckDBWriteSettings {
    /// Create a new `DuckDBWriteSettings` with default values
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set whether to recompute statistics on refresh
    #[must_use]
    pub fn with_recompute_statistics_on_refresh(mut self, enabled: bool) -> Self {
        self.recompute_statistics_on_refresh = enabled;
        self
    }

    /// Parse settings from  table creation parameters
    #[must_use]
    pub fn from_params(params: &HashMap<String, String>) -> Self {
        let mut settings = Self::default();

        if let Some(value) = params.get("on_refresh_recompute_statistics") {
            settings.recompute_statistics_on_refresh = match value.to_lowercase().as_str() {
                "true" | "enabled" => true,
                "false" | "disabled" => false,
                _ => {
                    tracing::warn!(
                "Invalid value for 'on_refresh_recompute_statistics': '{value}'. Expected 'enabled' or 'disabled'. Using default: {}",
                settings.recompute_statistics_on_refresh
                );
                    settings.recompute_statistics_on_refresh
                }
            };
        }

        settings
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_default_settings() {
        let settings = DuckDBWriteSettings::default();
        assert!(settings.recompute_statistics_on_refresh);
    }

    #[test]
    fn test_new_settings() {
        let settings = DuckDBWriteSettings::new();
        assert!(settings.recompute_statistics_on_refresh);
    }

    #[test]
    fn test_with_recompute_statistics_on_refresh() {
        let settings = DuckDBWriteSettings::new().with_recompute_statistics_on_refresh(false);
        assert!(!settings.recompute_statistics_on_refresh);
    }

    #[test]
    fn test_from_params_valid_enabled() {
        let mut params = HashMap::new();
        params.insert(
            "on_refresh_recompute_statistics".to_string(),
            "enabled".to_string(),
        );

        let settings = DuckDBWriteSettings::from_params(&params);
        assert!(settings.recompute_statistics_on_refresh);
    }

    #[test]
    fn test_from_params_valid_disabled() {
        let mut params = HashMap::new();
        params.insert(
            "on_refresh_recompute_statistics".to_string(),
            "disabled".to_string(),
        );

        let settings = DuckDBWriteSettings::from_params(&params);
        assert!(!settings.recompute_statistics_on_refresh);
    }

    #[test]
    fn test_from_params_invalid_value() {
        let mut params = HashMap::new();
        params.insert(
            "on_refresh_recompute_statistics".to_string(),
            "invalid".to_string(),
        );

        let settings = DuckDBWriteSettings::from_params(&params);
        // Should fall back to default (true) and log a warning
        assert!(settings.recompute_statistics_on_refresh);
    }

    #[test]
    fn test_from_params_missing_param() {
        let params = HashMap::new();

        let settings = DuckDBWriteSettings::from_params(&params);
        // Should use default value
        assert!(settings.recompute_statistics_on_refresh);
    }
}
