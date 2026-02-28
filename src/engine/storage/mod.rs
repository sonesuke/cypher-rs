//! Storage backend abstraction for the Cypher engine.
//!
//! This module provides a pluggable storage interface that allows different
//! data sources to be used with the query engine.

pub mod json;
pub mod memory;
pub mod storage_trait;

// Re-export commonly used types
pub use json::JsonStorage;
pub use memory::{MemoryStorage, MemoryStorageBuilder};
pub use storage_trait::{
    Storage, StorageError, StorageFeature, StorageMetadata, StorageResult, SyncStorage,
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::GraphConfig;
    use serde_json::json;

    #[test]
    fn test_storage_backends_interchangeable() {
        let data = json!({
            "users": [
                { "id": "1", "role": "admin" },
                { "id": "2", "role": "user" }
            ]
        });

        let config = GraphConfig {
            node_path: "users".to_string(),
            id_field: "id".to_string(),
            label_field: Some("role".to_string()),
            relation_fields: vec![],
        };

        // Test with JSON storage
        let json_storage = JsonStorage::from_value(data.clone());
        let json_graph = json_storage.load_graph_sync(&config).unwrap();
        assert_eq!(json_graph.nodes.len(), 2);

        // Test with memory storage
        let memory_storage = MemoryStorage::from_graph(json_graph.clone());
        let memory_graph = memory_storage.load_graph_sync(&config).unwrap();
        assert_eq!(memory_graph.nodes.len(), 2);
    }

    #[test]
    fn test_storage_metadata() {
        let json_storage = JsonStorage::from_value(json!({}));
        let json_metadata = json_storage.metadata();
        assert_eq!(json_metadata.name, "json");

        let memory_storage = MemoryStorage::empty();
        let memory_metadata = memory_storage.metadata();
        assert_eq!(memory_metadata.name, "memory");
    }
}
