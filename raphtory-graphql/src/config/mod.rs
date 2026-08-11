pub mod app_config;
pub mod auth_config;
pub mod cache_config;
pub mod concurrency_config;
pub mod log_config;
pub mod otlp_config;
pub mod parquet_config;
pub mod schema_config;

#[cfg(test)]
mod tests {
    use crate::config::{app_config::AppConfigBuilder, otlp_config::TracingLevel};
    use std::fs;
    use tempfile::NamedTempFile;

    #[test]
    fn test_load_config_from_toml() {
        let config_toml = r#"
            [logging]
            log_level = "DEBUG"

            [tracing]
            enabled = true
            level = "Essential"

            [cache]
            capacity = 20

            [auth]
            public_key = "MCowBQYDK2VwAyEADdrWr1kTLj+wSHlr45eneXmOjlHo3N1DjLIvDa2ozno="
        "#;
        let config_file = NamedTempFile::with_suffix(".toml").unwrap();
        let config_path = config_file.path();
        fs::write(&config_path, config_toml).unwrap();

        let result = AppConfigBuilder::new()
            .load_from_path(config_path)
            .unwrap()
            .build();

        let expected_config = AppConfigBuilder::new()
            .with_log_level("DEBUG".to_string())
            .with_tracing(true)
            .with_tracing_level(TracingLevel::ESSENTIAL)
            .with_cache_capacity(20)
            .with_auth_public_key(Some(
                "MCowBQYDK2VwAyEADdrWr1kTLj+wSHlr45eneXmOjlHo3N1DjLIvDa2ozno=".to_owned(),
            ))
            .unwrap()
            .build();

        assert_eq!(result, expected_config);
    }
}
