use std::collections::HashMap;

/// Configuration settings for DuckDB write operations
#[derive(Debug, Clone)]
pub struct DuckDBWriteSettings {
    /// Whether to execute ANALYZE statements after data write operations
    /// to update table statistics for query optimization
    pub recompute_statistics_on_write: bool,
    /// Whether an `InsertOp::Overwrite` on a file-backed instance writes into a
    /// fresh database file and atomically swaps it in (reclaiming disk space
    /// and leaving a checkpointed, WAL-free file) instead of rewriting the
    /// table inside the live file. See [`crate::file_swap`].
    ///
    /// This and [`Self::checkpoint_on_write`] are two answers to the same
    /// problem — a repeated overwrite never reclaiming the space of the
    /// generations it drops — with different costs, and they do not stack. On a
    /// file-backed instance this one takes over the whole overwrite, so
    /// `checkpoint_on_write` never runs; the replacement always produces a
    /// checkpointed file anyway. When it cannot apply (an in-memory instance)
    /// the overwrite falls back in place and `checkpoint_on_write` applies as
    /// usual.
    pub overwrite_file_swap: bool,
    /// Whether to execute a checkpoint after an overwrite completes.
    ///
    /// Cheaper than [`Self::overwrite_file_swap`], but it checkpoints the *live*
    /// instance: the plain `CHECKPOINT` fails while other transactions are open
    /// and escalates to `FORCE CHECKPOINT`, which aborts them. Prefer
    /// `overwrite_file_swap` where in-flight queries must not be interrupted.
    pub checkpoint_on_write: bool,
}

impl Default for DuckDBWriteSettings {
    fn default() -> Self {
        Self {
            recompute_statistics_on_write: true, // Enabled by default for better query performance
            overwrite_file_swap: false,
            checkpoint_on_write: false, // Disabled by default to avoid unnecessary overhead unless explicitly requested
        }
    }
}

impl DuckDBWriteSettings {
    /// Create a new `DuckDBWriteSettings` with default values
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set whether to recompute statistics on write
    #[must_use]
    pub fn with_recompute_statistics_on_write(mut self, enabled: bool) -> Self {
        self.recompute_statistics_on_write = enabled;
        self
    }

    /// Set whether overwrites swap in a freshly written database file
    #[must_use]
    pub fn with_overwrite_file_swap(mut self, enabled: bool) -> Self {
        self.overwrite_file_swap = enabled;
        self
    }

    /// Set whether to checkpoint the database after an overwrite completes
    #[must_use]
    pub fn with_checkpoint_on_write(mut self, enabled: bool) -> Self {
        self.checkpoint_on_write = enabled;
        self
    }

    /// Parse settings from  table creation parameters
    #[must_use]
    pub fn from_params(params: &HashMap<String, String>) -> Self {
        let mut settings = Self::default();

        if let Some(value) = params.get("recompute_statistics_on_write") {
            settings.recompute_statistics_on_write = match value.to_lowercase().as_str() {
                "true" | "enabled" => true,
                "false" | "disabled" => false,
                _ => {
                    tracing::warn!(
                "Invalid value for recompute statistics on write parameter: '{value}'. Expected 'enabled' or 'disabled'. Using default: {}",
                settings.recompute_statistics_on_write
                );
                    settings.recompute_statistics_on_write
                }
            };
        }

        if let Some(value) = params.get("overwrite_file_swap") {
            settings.overwrite_file_swap = match value.to_lowercase().as_str() {
                "true" | "enabled" => true,
                "false" | "disabled" => false,
                _ => {
                    tracing::warn!(
                        "Invalid value for overwrite file swap parameter: '{value}'. Expected 'enabled' or 'disabled'. Using default: {}",
                        settings.overwrite_file_swap
                    );
                    settings.overwrite_file_swap
                }
            };
        }

        if let Some(value) = params.get("checkpoint_on_write") {
            settings.checkpoint_on_write = match value.to_lowercase().as_str() {
                "true" | "enabled" => true,
                "false" | "disabled" => false,
                _ => {
                    tracing::warn!(
                        "Invalid value for checkpoint on write parameter: '{value}'. Expected one of 'enabled', 'disabled', 'true', 'false'. Using default: {}",
                        settings.checkpoint_on_write
                    );
                    settings.checkpoint_on_write
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
        assert!(settings.recompute_statistics_on_write);
    }

    #[test]
    fn test_new_settings() {
        let settings = DuckDBWriteSettings::new();
        assert!(settings.recompute_statistics_on_write);
    }

    #[test]
    fn test_with_recompute_statistics_on_write() {
        let settings = DuckDBWriteSettings::new().with_recompute_statistics_on_write(false);
        assert!(!settings.recompute_statistics_on_write);
    }

    #[test]
    fn test_from_params_valid_enabled() {
        let mut params = HashMap::new();
        params.insert(
            "recompute_statistics_on_write".to_string(),
            "enabled".to_string(),
        );

        let settings = DuckDBWriteSettings::from_params(&params);
        assert!(settings.recompute_statistics_on_write);
    }

    #[test]
    fn test_from_params_valid_disabled() {
        let mut params = HashMap::new();
        params.insert(
            "recompute_statistics_on_write".to_string(),
            "disabled".to_string(),
        );

        let settings = DuckDBWriteSettings::from_params(&params);
        assert!(!settings.recompute_statistics_on_write);
    }

    #[test]
    fn test_from_params_invalid_value() {
        let mut params = HashMap::new();
        params.insert(
            "recompute_statistics_on_write".to_string(),
            "invalid".to_string(),
        );

        let settings = DuckDBWriteSettings::from_params(&params);
        // Should fall back to default (true) and log a warning
        assert!(settings.recompute_statistics_on_write);
    }

    #[test]
    fn test_from_params_missing_param() {
        let params = HashMap::new();

        let settings = DuckDBWriteSettings::from_params(&params);
        // Should use default value
        assert!(settings.recompute_statistics_on_write);
        assert!(!settings.checkpoint_on_write);
    }

    #[test]
    fn test_with_checkpoint_on_write() {
        let settings = DuckDBWriteSettings::new().with_checkpoint_on_write(true);
        assert!(settings.checkpoint_on_write);
    }

    #[test]
    fn test_from_params_checkpoint_on_write() {
        let mut params = HashMap::new();
        params.insert("checkpoint_on_write".to_string(), "enabled".to_string());
        assert!(DuckDBWriteSettings::from_params(&params).checkpoint_on_write);

        params.insert("checkpoint_on_write".to_string(), "false".to_string());
        assert!(!DuckDBWriteSettings::from_params(&params).checkpoint_on_write);

        params.insert("checkpoint_on_write".to_string(), "bogus".to_string());
        // Should fall back to default (false) and log a warning
        assert!(!DuckDBWriteSettings::from_params(&params).checkpoint_on_write);
    }
}
