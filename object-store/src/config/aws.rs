use std::collections::HashMap;

use super::{str_is_truthy, ConfigError};

use object_store::aws::AmazonS3Builder;
use object_store::Result as ObjectStoreResult;
use once_cell::sync::Lazy;

#[derive(PartialEq, Eq)]
enum S3ConfigKey {
    AccessKeyId,
    SecretAccessKey,
    Region,
    DefaultRegion,
    Bucket,
    Endpoint,
    Token,
    VirtualHostedStyleRequest,
    MetadataEndpoint,
    Profile,
    AllowHttp,
}

impl S3ConfigKey {
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

static ALIAS_MAP: Lazy<HashMap<&'static str, S3ConfigKey>> = Lazy::new(|| {
    HashMap::from([
        // access key id
        ("aws_access_key_id", S3ConfigKey::AccessKeyId),
        ("access_key_id", S3ConfigKey::AccessKeyId),
        // secret access key
        ("aws_secret_access_key", S3ConfigKey::SecretAccessKey),
        ("secret_access_key", S3ConfigKey::SecretAccessKey),
        // default region
        ("aws_default_region", S3ConfigKey::DefaultRegion),
        ("default_region", S3ConfigKey::DefaultRegion),
        // region
        ("aws_region", S3ConfigKey::Region),
        ("region", S3ConfigKey::Region),
        // region
        ("aws_bucket", S3ConfigKey::Bucket),
        ("bucket", S3ConfigKey::Bucket),
        // custom S3 endpoint
        ("aws_endpoint_url", S3ConfigKey::Endpoint),
        ("aws_endpoint", S3ConfigKey::Endpoint),
        ("endpoint_url", S3ConfigKey::Endpoint),
        ("endpoint", S3ConfigKey::Endpoint),
        // session token
        ("aws_session_token", S3ConfigKey::Token),
        ("session_token", S3ConfigKey::Token),
        // virtual hosted style request
        (
            "aws_virtual_hosted_style_request",
            S3ConfigKey::VirtualHostedStyleRequest,
        ),
        (
            "virtual_hosted_style_request",
            S3ConfigKey::VirtualHostedStyleRequest,
        ),
        // profile
        ("aws_profile", S3ConfigKey::Profile),
        ("profile", S3ConfigKey::Profile),
        // metadata endpoint
        ("aws_metadata_endpoint", S3ConfigKey::MetadataEndpoint),
        ("metadata_endpoint", S3ConfigKey::MetadataEndpoint),
    ])
});

pub(crate) struct S3Config {
    /// AWS Access Key Id
    access_key_id: Option<String>,
    /// AWS Secret Access Key
    secret_access_key: Option<String>,
    /// region (e.g. `us-east-1`) (required)
    region: Option<String>,
    /// bucket_name (required)
    bucket: Option<String>,
    default_region: Option<String>,
    /// Endpoint for communicating with AWS S3. Default value is based on region.
    /// The `endpoint` field should be consistent with the field `virtual_hosted_style_request'.
    endpoint: Option<String>,
    /// token to use for requests (passed to underlying provider)
    token: Option<String>,
    /// If `virtual_hosted_style_request` is :
    /// * false (default):  Path style request is used
    /// * true:  Virtual hosted style request is used
    ///
    /// If the `endpoint` is provided then it should be
    /// consistent with `virtual_hosted_style_request`.
    /// i.e. if `virtual_hosted_style_request` is set to true
    /// then `endpoint` should have bucket name included.
    virtual_hosted_style_request: Option<bool>,
    metadata_endpoint: Option<String>,
    profile: Option<String>,
    allow_http: Option<bool>,
}

impl S3Config {
    fn new(options: &HashMap<String, String>) -> Self {
        let mut access_key_id = None;
        let mut secret_access_key = None;
        let mut region = None;
        let mut default_region = None;
        let mut bucket = None;
        let mut endpoint = None;
        let mut token = None;
        let mut virtual_hosted_style_request = None;
        let mut metadata_endpoint = None;
        let mut profile = None;
        let mut allow_http = None;

        for (raw, value) in options {
            if let Some(key) = ALIAS_MAP.get(&*raw.to_ascii_lowercase()) {
                match key {
                    S3ConfigKey::AccessKeyId => access_key_id = Some(value.clone()),
                    S3ConfigKey::SecretAccessKey => secret_access_key = Some(value.clone()),
                    S3ConfigKey::Region => region = Some(value.clone()),
                    S3ConfigKey::DefaultRegion => default_region = Some(value.clone()),
                    S3ConfigKey::Bucket => bucket = Some(value.clone()),
                    S3ConfigKey::Endpoint => endpoint = Some(value.clone()),
                    S3ConfigKey::Token => token = Some(value.clone()),
                    S3ConfigKey::VirtualHostedStyleRequest => {
                        virtual_hosted_style_request = Some(str_is_truthy(value))
                    }
                    S3ConfigKey::MetadataEndpoint => metadata_endpoint = Some(value.clone()),
                    S3ConfigKey::Profile => profile = Some(value.clone()),
                    S3ConfigKey::AllowHttp => allow_http = Some(str_is_truthy(value)),
                }
            }
        }

        Self {
            access_key_id,
            secret_access_key,
            region,
            default_region,
            bucket,
            endpoint,
            token,
            virtual_hosted_style_request,
            metadata_endpoint,
            profile,
            allow_http,
        }
    }

    fn get_value(&self, key: S3ConfigKey) -> Option<String> {
        match key {
            S3ConfigKey::AccessKeyId => self.access_key_id.clone().or(key.get_from_env()),
            S3ConfigKey::SecretAccessKey => self.secret_access_key.clone().or(key.get_from_env()),
            S3ConfigKey::Region => self
                .region
                .clone()
                .or(key.get_from_env())
                .or(self.default_region.clone())
                .or(S3ConfigKey::DefaultRegion.get_from_env()),
            S3ConfigKey::DefaultRegion => self.default_region.clone().or(key.get_from_env()),
            S3ConfigKey::Bucket => self.bucket.clone().or(key.get_from_env()),
            S3ConfigKey::Endpoint => self.endpoint.clone().or(key.get_from_env()),
            S3ConfigKey::Token => self.token.clone().or(key.get_from_env()),
            S3ConfigKey::VirtualHostedStyleRequest => self
                .virtual_hosted_style_request
                .clone()
                .map(|v| v.to_string())
                .or(key.get_from_env()),
            S3ConfigKey::MetadataEndpoint => self.metadata_endpoint.clone().or(key.get_from_env()),
            S3ConfigKey::Profile => self.profile.clone().or(key.get_from_env()),
            S3ConfigKey::AllowHttp => self
                .allow_http
                .clone()
                .map(|v| v.to_string())
                .or(key.get_from_env()),
        }
    }

    /// Check all options if a valid builder can be generated, if not, check if configuration
    /// can be read from the environment.
    pub fn get_builder(
        url: impl Into<String>,
        options: &HashMap<String, String>,
    ) -> ObjectStoreResult<AmazonS3Builder> {
        let config = Self::new(options);

        let mut builder = AmazonS3Builder::default()
            .with_url(url)
            .with_region(
                config
                    .get_value(S3ConfigKey::Region)
                    .ok_or(ConfigError::required("aws region must be specified."))?,
            )
            .with_bucket_name(
                config
                    .get_value(S3ConfigKey::Bucket)
                    .ok_or(ConfigError::required("aws bucket must be specified."))?,
            );

        if let Some(endpoint) = config.get_value(S3ConfigKey::Endpoint) {
            builder = builder.with_endpoint(endpoint);
        }

        if let Some(value) = config.get_value(S3ConfigKey::VirtualHostedStyleRequest) {
            builder = builder.with_virtual_hosted_style_request(str_is_truthy(&value));
        }

        if let Some(allow_http) = config.get_value(S3ConfigKey::AllowHttp) {
            builder = builder.with_allow_http(str_is_truthy(&allow_http));
        }

        if let (Some(access_key_id), Some(secret_access_key)) = (
            config.get_value(S3ConfigKey::AccessKeyId),
            config.get_value(S3ConfigKey::SecretAccessKey),
        ) {
            builder = builder
                .with_access_key_id(access_key_id)
                .with_secret_access_key(secret_access_key);
        }

        Ok(builder)
    }
}
