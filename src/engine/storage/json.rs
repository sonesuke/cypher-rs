use super::storage_trait::{
    StorageError, StorageFeature, StorageMetadata, StorageResult, SyncStorage,
};
use crate::config::{GraphConfig, RootObjectConfig};
use crate::graph::{Edge, Graph, Node};
use serde_json::Value;
use std::fs;
use std::path::Path;
use std::sync::Arc;

/// JSON-based storage backend.
///
/// Loads graph data from a JSON file or value.
#[derive(Debug, Clone)]
pub struct JsonStorage {
    /// The JSON data containing the graph
    data: Arc<Value>,
    /// Storage metadata
    metadata: StorageMetadata,
}

impl JsonStorage {
    /// Create a new JsonStorage from a JSON value.
    pub fn from_value(data: Value) -> Self {
        let metadata = StorageMetadata::new("json", "1.0.0")
            .with_feature(StorageFeature::ConcurrentReads)
            .with_property("data_type", "json");

        Self {
            data: Arc::new(data),
            metadata,
        }
    }

    /// Create a new JsonStorage from a JSON file.
    pub fn from_file<P: AsRef<Path>>(path: P) -> StorageResult<Self> {
        let content = fs::read_to_string(&path)?;
        let data: Value = serde_json::from_str(&content)?;

        let mut metadata = StorageMetadata::new("json", "1.0.0")
            .with_feature(StorageFeature::ConcurrentReads)
            .with_feature(StorageFeature::Persistence);

        if let Some(path_str) = path.as_ref().to_str() {
            metadata = metadata.with_property("source_file", path_str);
        }

        Ok(Self {
            data: Arc::new(data),
            metadata,
        })
    }

    /// Create a new JsonStorage from a JSON string.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(json_str: &str) -> StorageResult<Self> {
        let data: Value = serde_json::from_str(json_str)?;
        Ok(Self::from_value(data))
    }

    /// Get a reference to the underlying JSON data.
    pub fn data(&self) -> &Value {
        &self.data
    }
}

impl SyncStorage for JsonStorage {
    fn load_graph_sync(&self, config: &GraphConfig) -> StorageResult<Graph> {
        build_graph_from_json(&self.data, config)
    }

    fn get_node_sync(&self, _id: &str) -> StorageResult<Option<Node>> {
        // For JSON storage, we need to rebuild the graph to find the node
        // This is inefficient, but provides a consistent interface
        // In a real implementation, you might want to cache the graph
        Ok(None)
    }

    fn metadata(&self) -> StorageMetadata {
        self.metadata.clone()
    }

    fn supports_feature(&self, feature: StorageFeature) -> bool {
        self.metadata.features.contains(&feature)
    }
}

/// Build a graph from a JSON value using the given configuration.
pub fn build_graph_from_json(json: &Value, config: &GraphConfig) -> StorageResult<Graph> {
    // If root_object_config is present, use root object mode
    if let Some(root_config) = &config.root_object_config {
        return build_graph_from_root_object(json, root_config);
    }

    // Otherwise, use the standard array-based mode
    let mut graph = Graph::new();

    // Navigate to the node array
    let nodes_array = navigate_json_path(json, &config.node_path).ok_or_else(|| {
        StorageError::InvalidData(format!("Cannot find node path: {}", config.node_path))
    })?;

    let nodes_arr = nodes_array
        .as_array()
        .ok_or_else(|| StorageError::InvalidData("Node path is not an array".to_string()))?;

    // First pass: add all nodes
    for (idx, node_json) in nodes_arr.iter().enumerate() {
        let id = node_json
            .get(&config.id_field)
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                StorageError::InvalidData(format!(
                    "Node at index {} missing id field: {}",
                    idx, config.id_field
                ))
            })?
            .to_string();

        let label = config.label_field.as_ref().and_then(|field| {
            node_json
                .get(field)
                .and_then(|v| v.as_str())
                .map(String::from)
        });

        let node = Node::new(id.clone(), label, node_json.clone());
        graph.add_node(node);
    }

    // Second pass: add edges from relation fields
    for (idx, node_json) in nodes_arr.iter().enumerate() {
        let from_id = node_json
            .get(&config.id_field)
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                StorageError::InvalidData(format!(
                    "Node at index {} missing id field: {}",
                    idx, config.id_field
                ))
            })?;

        let from_idx = graph
            .get_node_index(from_id)
            .ok_or_else(|| StorageError::InvalidData(format!("Cannot find node: {}", from_id)))?;

        for rel_field in &config.relation_fields {
            if let Some(rel_value) = node_json.get(rel_field) {
                if let Some(rel_array) = rel_value.as_array() {
                    for to_id_val in rel_array {
                        if let Some(to_id) = to_id_val.as_str() {
                            if let Some(to_idx) = graph.get_node_index(to_id) {
                                let edge = Edge::new(from_idx, to_idx, rel_field.clone());
                                graph.add_edge(edge);
                            }
                        }
                    }
                } else if let Some(to_id) = rel_value.as_str() {
                    if let Some(to_idx) = graph.get_node_index(to_id) {
                        let edge = Edge::new(from_idx, to_idx, rel_field.clone());
                        graph.add_edge(edge);
                    }
                }
            }
        }
    }

    Ok(graph)
}

/// Build a graph from a root object JSON value.
///
/// Creates a root node from the root object, then creates related nodes
/// for each nested array and connects them with HAS_CHILD relationships.
fn build_graph_from_root_object(json: &Value, config: &RootObjectConfig) -> StorageResult<Graph> {
    let mut graph = Graph::new();

    // Get the root object
    let root_obj = json
        .as_object()
        .ok_or_else(|| StorageError::InvalidData("Root is not an object".to_string()))?;

    // Extract root node ID
    let root_id = root_obj
        .get(&config.id_field)
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            StorageError::InvalidData(format!("Root object missing id field: {}", config.id_field))
        })?
        .to_string();

    // Extract root node label (if specified)
    let root_label = config
        .label_field
        .as_ref()
        .and_then(|field| root_obj.get(field))
        .and_then(|v| v.as_str())
        .or(Some(config.primary_label.as_str()))
        .map(String::from);

    // Create the root node
    let root_node = Node::new(root_id.clone(), root_label.clone(), json.clone());
    let root_idx = graph.add_node(root_node);

    // Process each related node array
    for related_array_config in &config.related_node_arrays {
        if let Some(array_value) = root_obj.get(&related_array_config.field_name) {
            if let Some(arr) = array_value.as_array() {
                // Create nodes for each element in the array
                for (idx, element) in arr.iter().enumerate() {
                    // Get the element's ID
                    let element_id = element
                        .get(&related_array_config.id_field)
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| {
                            StorageError::InvalidData(format!(
                                "Element in {} at index {} missing id field: {}",
                                related_array_config.field_name, idx, related_array_config.id_field
                            ))
                        })?
                        .to_string();

                    // Get the element's label (if configured)
                    let element_label =
                        related_array_config.label_field.as_ref().and_then(|field| {
                            element
                                .get(field)
                                .and_then(|v| v.as_str())
                                .map(String::from)
                        });

                    // Create and add the related node
                    let related_node =
                        Node::new(element_id.clone(), element_label, element.clone());
                    let related_idx = graph.add_node(related_node);

                    // Create edge from root to related node
                    let rel_type = related_array_config.relationship_type.clone();
                    #[cfg(debug_assertions)]
                    println!(
                        "Creating edge: root_idx={}, related_idx={}, rel_type='{}'",
                        root_idx, related_idx, rel_type
                    );
                    let edge = Edge::new(root_idx, related_idx, rel_type);
                    #[cfg(debug_assertions)]
                    println!(
                        "Edge created: from={}, to={}, rel_type='{}'",
                        edge.from, edge.to, edge.rel_type
                    );
                    graph.add_edge(edge);
                }
            }
        }
    }

    Ok(graph)
}

/// Navigate a JSON path to retrieve a value.
///
/// Supports dot-notation paths like "data.users" or "users".
fn navigate_json_path<'a>(json: &'a Value, path: &str) -> Option<&'a Value> {
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = json;

    for part in parts {
        current = match part {
            "*" => current, // Wildcard - keep current
            _ => current.get(part)?,
        };
    }

    Some(current)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_json_storage_from_value() {
        let data = json!({
            "users": [
                { "id": "1", "role": "admin", "age": 30 },
                { "id": "2", "role": "user", "age": 25 }
            ]
        });

        let storage = JsonStorage::from_value(data);
        let config = GraphConfig {
            node_path: "users".to_string(),
            id_field: "id".to_string(),
            label_field: Some("role".to_string()),
            relation_fields: vec![],
            root_object_config: None,
        };

        let graph = storage.load_graph_sync(&config).unwrap();
        assert_eq!(graph.nodes.len(), 2);
    }

    #[test]
    fn test_json_storage_from_str() {
        let json_str = r#"{
            "users": [
                { "id": "1", "role": "admin" },
                { "id": "2", "role": "user" }
            ]
        }"#;

        let storage = JsonStorage::from_str(json_str).unwrap();
        let config = GraphConfig::minimal("users", "id");
        let graph = storage.load_graph_sync(&config).unwrap();
        assert_eq!(graph.nodes.len(), 2);
    }

    #[test]
    fn test_navigate_json_path() {
        let data = json!({
            "data": {
                "users": [
                    { "id": "1" }
                ]
            }
        });

        let result = navigate_json_path(&data, "data.users");
        assert!(result.is_some());
        assert!(result.unwrap().is_array());

        let result = navigate_json_path(&data, "data.nonexistent");
        assert!(result.is_none());
    }

    #[test]
    fn test_json_storage_with_relations() {
        let data = json!({
            "users": [
                { "id": "1", "friends": ["2", "3"] },
                { "id": "2", "friends": ["1"] },
                { "id": "3", "friends": [] }
            ]
        });

        let storage = JsonStorage::from_value(data);
        let config = GraphConfig {
            node_path: "users".to_string(),
            id_field: "id".to_string(),
            label_field: None,
            relation_fields: vec!["friends".to_string()],
            root_object_config: None,
        };

        let graph = storage.load_graph_sync(&config).unwrap();
        assert_eq!(graph.nodes.len(), 3);
        assert_eq!(graph.edges.len(), 3); // 1->2, 1->3, 2->1
    }

    #[test]
    fn test_json_storage_metadata() {
        let data = json!({"users": []});
        let storage = JsonStorage::from_value(data);
        let metadata = storage.metadata();

        assert_eq!(metadata.name, "json");
        assert!(metadata.features.contains(&StorageFeature::ConcurrentReads));
    }
}
