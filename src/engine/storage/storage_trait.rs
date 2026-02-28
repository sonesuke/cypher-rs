use crate::graph::{Graph, Node};
use crate::config::GraphConfig;
use async_trait::async_trait;

/// Type alias for optional node result to avoid >> parsing issues
pub type OptionalNodeResult = StorageResult<Option<Node>>;

/// Result type for storage operations
pub type StorageResult<T> = std::result::Result<T, StorageError>;

/// Errors that can occur during storage operations
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON parsing error: {0}")]
    JsonParse(#[from] serde_json::Error),

    #[error("Invalid data structure: {0}")]
    InvalidData(String),

    #[error("Node not found: {0}")]
    NodeNotFound(String),

    #[error("Configuration error: {0}")]
    ConfigError(String),
}

/// Abstract storage backend for graph data.
///
/// This trait allows different storage implementations (JSON files,
/// in-memory, database, etc.) to be used interchangeably with the
/// Cypher query engine.
#[async_trait]
pub trait Storage: Send + Sync {
    /// Load a graph from the storage backend.
    fn load_graph(&self, config: &GraphConfig) -> StorageResult<Graph>;

    /// Get a node by its ID.
    fn get_node(&self, id: &str) -> OptionalNodeResult;
}

/// Features that a storage backend may support.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageFeature {
    /// Support for concurrent reads
    ConcurrentReads,
    /// Support for concurrent writes
    ConcurrentWrites,
    /// Support for transactions
    Transactions,
    /// Support for querying subsets of data
    PartialQuery,
    /// Support for persistent storage
    Persistence,
}

/// Metadata about a storage backend.
#[derive(Debug, Clone)]
pub struct StorageMetadata {
    /// Name of the storage backend
    pub name: String,
    /// Version of the storage backend
    pub version: String,
    /// Supported features
    pub features: Vec<StorageFeature>,
    /// Additional properties
    pub properties: std::collections::HashMap<String, String>,
}

impl StorageMetadata {
    /// Create new storage metadata.
    pub fn new(name: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version: version.into(),
            features: Vec::new(),
            properties: std::collections::HashMap::new(),
        }

    }

    /// Add a feature to the metadata.
    pub fn with_feature(mut self, feature: StorageFeature) -> Self {
        self.features.push(feature);
        self
    }

    /// Add a property to the metadata.
    pub fn with_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }
}

/// A helper trait for synchronous storage operations.
///
/// This is provided for convenience when working with storage
/// backends that don't require async operations.
pub trait SyncStorage: Send + Sync {
    /// Load a graph synchronously from the storage backend.
    fn load_graph_sync(&self, config: &GraphConfig) -> StorageResult<Graph>;

    /// Get a node by its ID synchronously.
    fn get_node_sync(&self, id: &str) -> StorageResult<Option<Node>>;

    /// Get storage metadata.
    fn metadata(&self) -> StorageMetadata;

    /// Check if the storage backend supports a specific feature.
    fn supports_feature(&self, feature: StorageFeature) -> bool;
}

// Blanket implementation of Storage for SyncStorage
#[async_trait]
impl<T: SyncStorage + ?Sized> Storage for T {
    fn load_graph(&self, config: &GraphConfig) -> StorageResult<Graph> {
        self.load_graph_sync(config)
    }

    fn get_node(&self, id: &str) -> OptionalNodeResult {
        self.get_node_sync(id)
    }
}
