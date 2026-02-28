use super::storage_trait::{StorageError, StorageResult, StorageFeature, StorageMetadata, SyncStorage};
use crate::config::GraphConfig;
use crate::graph::{Graph, Node};
use std::sync::Arc;

/// In-memory storage backend.
///
/// Stores graph data directly in memory without persistence.
/// Useful for testing and scenarios where persistence is not required.
#[derive(Debug, Clone)]
pub struct MemoryStorage {
    /// The cached graph
    graph: Arc<Graph>,
    /// Storage metadata
    metadata: StorageMetadata,
}

impl MemoryStorage {
    /// Create a new MemoryStorage from a Graph.
    pub fn from_graph(graph: Graph) -> Self {
        let metadata = StorageMetadata::new("memory", "1.0.0")
            .with_feature(StorageFeature::ConcurrentReads)
            .with_feature(StorageFeature::ConcurrentWrites)
            .with_property("volatile", "true");

        Self {
            graph: Arc::new(graph),
            metadata,
        }
    }

    /// Create a new empty MemoryStorage.
    pub fn empty() -> Self {
        Self::from_graph(Graph::new())
    }

    /// Get a reference to the underlying graph.
    pub fn graph(&self) -> &Graph {
        &self.graph
    }

    /// Create a MemoryStorage with a pre-populated graph.
    ///
    /// # Example
    ///
    /// ```rust
    /// use cypher_rs::engine::storage::MemoryStorage;
    /// use cypher_rs::graph::{Graph, Node};
    /// use serde_json::json;
    ///
    /// let mut graph = Graph::new();
    /// graph.add_node(Node::new("1".to_string(), Some("User".to_string()), json!({"name": "Alice"})));
    /// graph.add_node(Node::new("2".to_string(), Some("User".to_string()), json!({"name": "Bob"})));
    ///
    /// let storage = MemoryStorage::from_graph(graph);
    /// ```
    pub fn with_nodes(nodes: Vec<Node>) -> StorageResult<Self> {
        let mut graph = Graph::new();
        for node in nodes {
            graph.add_node(node);
        }
        Ok(Self::from_graph(graph))
    }
}

impl SyncStorage for MemoryStorage {
    fn load_graph_sync(&self, _config: &GraphConfig) -> StorageResult<Graph> {
        // Return a clone of the cached graph
        // Note: This clones the Graph structure but not the Node data (which uses Arc internally)
        Ok(Graph {
            nodes: self.graph.nodes.clone(),
            edges: self.graph.edges.clone(),
            id_map: self.graph.id_map.clone(),
        })
    }

    fn get_node_sync(&self, id: &str) -> StorageResult<Option<Node>> {
        Ok(self.graph.get_node(id).cloned())
    }

    fn metadata(&self) -> StorageMetadata {
        self.metadata.clone()
    }

    fn supports_feature(&self, feature: StorageFeature) -> bool {
        self.metadata.features.contains(&feature)
    }
}

/// A builder for MemoryStorage.
///
/// Provides a fluent interface for constructing in-memory graph storage.
#[derive(Debug, Default)]
pub struct MemoryStorageBuilder {
    graph: Graph,
}

impl MemoryStorageBuilder {
    /// Create a new builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a node to the storage.
    pub fn add_node(mut self, node: Node) -> Self {
        self.graph.add_node(node);
        self
    }

    /// Add multiple nodes to the storage.
    pub fn add_nodes(mut self, nodes: Vec<Node>) -> Self {
        for node in nodes {
            self.graph.add_node(node);
        }
        self
    }

    /// Build the MemoryStorage.
    pub fn build(self) -> MemoryStorage {
        MemoryStorage::from_graph(self.graph)
    }
}

impl Clone for MemoryStorageBuilder {
    fn clone(&self) -> Self {
        Self {
            graph: Graph {
                nodes: self.graph.nodes.clone(),
                edges: self.graph.edges.clone(),
                id_map: self.graph.id_map.clone(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_memory_storage_empty() {
        let storage = MemoryStorage::empty();
        let config = GraphConfig::minimal("users", "id");
        let graph = storage.load_graph_sync(&config).unwrap();
        assert_eq!(graph.nodes.len(), 0);
    }

    #[test]
    fn test_memory_storage_from_graph() {
        let mut graph = Graph::new();
        graph.add_node(Node::new(
            "1".to_string(),
            Some("User".to_string()),
            json!({"name": "Alice"}),
        ));
        graph.add_node(Node::new(
            "2".to_string(),
            Some("User".to_string()),
            json!({"name": "Bob"}),
        ));

        let storage = MemoryStorage::from_graph(graph);
        let loaded_graph = storage.load_graph_sync(&GraphConfig::default()).unwrap();
        assert_eq!(loaded_graph.nodes.len(), 2);
    }

    #[test]
    fn test_memory_storage_get_node() {
        let mut graph = Graph::new();
        graph.add_node(Node::new(
            "1".to_string(),
            Some("User".to_string()),
            json!({"name": "Alice"}),
        ));

        let storage = MemoryStorage::from_graph(graph);
        let node = storage.get_node_sync("1").unwrap();
        assert!(node.is_some());
        assert_eq!(node.unwrap().id, "1");
    }

    #[test]
    fn test_memory_storage_builder() {
        let storage = MemoryStorageBuilder::new()
            .add_node(Node::new(
                "1".to_string(),
                Some("User".to_string()),
                json!({"name": "Alice"}),
            ))
            .add_node(Node::new(
                "2".to_string(),
                Some("User".to_string()),
                json!({"name": "Bob"}),
            ))
            .build();

        let graph = storage.load_graph_sync(&GraphConfig::default()).unwrap();
        assert_eq!(graph.nodes.len(), 2);
    }

    #[test]
    fn test_memory_storage_metadata() {
        let storage = MemoryStorage::empty();
        let metadata = storage.metadata();

        assert_eq!(metadata.name, "memory");
        assert!(metadata.features.contains(&StorageFeature::ConcurrentReads));
        assert!(metadata.features.contains(&StorageFeature::ConcurrentWrites));
        assert_eq!(metadata.properties.get("volatile"), Some(&"true".to_string()));
    }

    #[test]
    fn test_memory_storage_supports_feature() {
        let storage = MemoryStorage::empty();
        assert!(storage.supports_feature(StorageFeature::ConcurrentReads));
        assert!(storage.supports_feature(StorageFeature::ConcurrentWrites));
        assert!(!storage.supports_feature(StorageFeature::Persistence));
    }
}
