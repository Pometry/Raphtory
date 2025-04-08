pub mod app_config;
pub mod auth_config;
pub mod cache_config;
pub mod log_config;
pub mod otlp_config;

#[cfg(test)]
mod tests {
    use crate::config::app_config::{load_config, AppConfigBuilder};
    use std::{fs, path::PathBuf};

    #[test]
    fn test_load_config_from_toml() {
        let config_toml = r#"
            [logging]
            log_level = "DEBUG"

            [tracing]
            tracing_enabled = true

            [cache]
            tti_seconds = 1000

            [auth]
            public_key = "MCowBQYDK2VwAyEADdrWr1kTLj+wSHlr45eneXmOjlHo3N1DjLIvDa2ozno="
        "#;
        let config_path = PathBuf::from("test_config.toml");
        fs::write(&config_path, config_toml).unwrap();

        let result = load_config(None, Some(config_path.clone()));
        let expected_config = AppConfigBuilder::new()
            .with_log_level("DEBUG".to_string())
            .with_tracing(true)
            .with_cache_capacity(30)
            .with_cache_tti_seconds(1000)
            .with_auth_public_key(Some(
                "MCowBQYDK2VwAyEADdrWr1kTLj+wSHlr45eneXmOjlHo3N1DjLIvDa2ozno=".to_owned(),
            ))
            .unwrap()
            .build();

        assert_eq!(result.unwrap(), expected_config);

        // Cleanup: delete the test TOML file
        fs::remove_file(config_path).unwrap();
    }

    #[test]
    fn test_load_config_with_custom_cache() {
        let app_config = AppConfigBuilder::new()
            .with_cache_capacity(50)
            .with_cache_tti_seconds(1200)
            .build();

        let result = load_config(Some(app_config.clone()), None);

        assert_eq!(result.unwrap(), app_config);
    }
}
