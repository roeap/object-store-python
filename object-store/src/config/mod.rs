use object_store::Error as ObjectStoreError;

pub(crate) mod aws;
pub(crate) mod azure;
pub(crate) mod google;

#[derive(Debug, thiserror::Error)]
enum ConfigError {
    #[error("Missing configuration {0}")]
    Required(String),
    #[error("Failed to find valid credential.")]
    MissingCredential,
    #[error("Failed to decode SAS key: {0}\nSAS keys must be percent-encoded. They come encoded in the Azure portal and Azure Storage Explorer.")]
    Decode(String),
}

impl ConfigError {
    pub fn required(message: impl Into<String>) -> Self {
        ConfigError::Required(message.into())
    }
}

impl From<ConfigError> for ObjectStoreError {
    fn from(err: ConfigError) -> Self {
        ObjectStoreError::Generic {
            store: "Generic",
            source: Box::new(err),
        }
    }
}

#[allow(dead_code)]
pub(crate) fn str_is_truthy(val: &str) -> bool {
    val == "1"
        || val.to_lowercase() == "true"
        || val.to_lowercase() == "on"
        || val.to_lowercase() == "yes"
        || val.to_lowercase() == "y"
}
