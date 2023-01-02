use std::collections::HashMap;

use super::ConfigError;

use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::Result as ObjectStoreResult;
use once_cell::sync::Lazy;

#[derive(PartialEq, Eq)]
enum GoogleConfigKey {
    ServiceAccount,
}

impl GoogleConfigKey {
    fn get_from_env(&self) -> Option<String> {
        for (key, value) in ALIAS_MAP.iter() {
            if value == self {
                if let Ok(val) = std::env::var(key.to_ascii_uppercase()) {
                    return Some(val);
                }
            }
        }
        None
    }
}

static ALIAS_MAP: Lazy<HashMap<&'static str, GoogleConfigKey>> = Lazy::new(|| {
    HashMap::from([
        // service account
        ("google_service_account", GoogleConfigKey::ServiceAccount),
        ("service_account", GoogleConfigKey::ServiceAccount),
    ])
});

pub(crate) struct GoogleConfig {
    service_account: Option<String>,
}

impl GoogleConfig {
    fn new(options: &HashMap<String, String>) -> Self {
        let mut service_account = None;

        for (raw, value) in options {
            if let Some(key) = ALIAS_MAP.get(&*raw.to_ascii_lowercase()) {
                match key {
                    GoogleConfigKey::ServiceAccount => service_account = Some(value.clone()),
                }
            }
        }

        Self { service_account }
    }

    fn get_value(&self, key: GoogleConfigKey) -> Option<String> {
        match key {
            GoogleConfigKey::ServiceAccount => self.service_account.clone().or(key.get_from_env()),
        }
    }

    /// Check all options if a valid builder can be generated, if not, check if configuration
    /// can be read from the environment.
    pub fn get_builder(
        url: impl Into<String>,
        options: &HashMap<String, String>,
    ) -> ObjectStoreResult<GoogleCloudStorageBuilder> {
        let config = Self::new(options);
        let builder = GoogleCloudStorageBuilder::default()
            .with_url(url)
            .with_service_account_path(config.get_value(GoogleConfigKey::ServiceAccount).ok_or(
                ConfigError::required("google service account must be specified."),
            )?);

        Ok(builder)
    }
}
