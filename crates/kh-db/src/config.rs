/// Connection settings for the Postgres pool.
#[derive(Debug, Clone)]
pub struct DbConfig {
    pub url: String,
    pub max_connections: u32,
}

impl DbConfig {
    pub const DEFAULT_MAX_CONNECTIONS: u32 = 10;

    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            max_connections: Self::DEFAULT_MAX_CONNECTIONS,
        }
    }

    pub fn max_connections(mut self, max_connections: u32) -> Self {
        self.max_connections = max_connections;
        self
    }
}
