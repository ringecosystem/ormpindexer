use std::{env, fmt};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeConfig {
    pub datalens_endpoint: String,
    pub datalens_application: String,
    pub datalens_token: Option<SecretString>,
    pub database_url: Option<SecretString>,
}

impl RuntimeConfig {
    pub fn from_env() -> Self {
        Self {
            datalens_endpoint: optional_env("ORMPINDEXER_DATALENS_ENDPOINT")
                .unwrap_or_else(|| "http://localhost:8080".to_owned()),
            datalens_application: optional_env("ORMPINDEXER_DATALENS_APPLICATION")
                .unwrap_or_else(|| "ormpindexer".to_owned()),
            datalens_token: optional_env("ORMPINDEXER_DATALENS_TOKEN").map(SecretString::new),
            database_url: optional_env("ORMPINDEXER_DATABASE_URL").map(SecretString::new),
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct SecretString(String);

impl SecretString {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted>")
    }
}

fn optional_env(key: &str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}
