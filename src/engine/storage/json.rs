use super::storage_trait::{
    StorageError, StorageFeature, StorageMetadata, StorageResult, SyncStorage,
};
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
    fn load_graph_sync(&self) -> StorageResult<Graph> {
        build_graph_from_root_object(&self.data, "Root")
    }

    fn get_node_sync(&self, _id: &str) -> StorageResult<Option<crate::graph::Node>> {
        Ok(None)
    }

    fn metadata(&self) -> StorageMetadata {
        self.metadata.clone()
    }

    fn supports_feature(&self, feature: StorageFeature) -> bool {
        self.metadata.features.contains(&feature)
    }
}

/// Build a graph from a root object JSON value.
///
/// Creates a root node from the root object, then creates related nodes
/// for each nested array-of-objects or object value, connecting them with
/// relationships derived from the field name. Non-scalar fields (arrays
/// and objects) are removed from the root node's data since they are
/// accessed via relationships.
pub fn build_graph_from_root_object(json: &Value, root_label: &str) -> StorageResult<Graph> {
    let mut graph = Graph::new();

    let root_obj = json
        .as_object()
        .ok_or_else(|| StorageError::InvalidData("Root is not an object".to_string()))?;

    // Extract root node ID
    let root_id = root_obj
        .get("id")
        .or_else(|| root_obj.get("_id"))
        .and_then(|v| v.as_str())
        .unwrap_or("root")
        .to_string();

    // Build root node data with scalar fields only
    let mut root_data = serde_json::Map::new();
    for (key, value) in root_obj {
        if value.is_string() || value.is_number() || value.is_boolean() {
            root_data.insert(key.clone(), value.clone());
        }
    }

    let root_node = Node::new(
        root_id.clone(),
        Some(root_label.to_string()),
        Value::Object(root_data),
    );
    let root_idx = graph.add_node(root_node);

    // Process each field: arrays of objects and object values become child nodes
    for (field_name, field_value) in root_obj {
        match field_value {
            Value::Array(arr) => {
                if arr.is_empty() {
                    continue;
                }
                if let Some(first) = arr.first() {
                    if !first.is_object() {
                        continue;
                    }

                    for (idx, element) in arr.iter().enumerate() {
                        if let Value::Object(obj) = element {
                            let eid = obj
                                .get("id")
                                .or_else(|| obj.get("_id"))
                                .and_then(|v| v.as_str())
                                .map(String::from)
                                .unwrap_or_else(|| format!("{}-{}", field_name, idx));

                            let elabel = obj
                                .get("type")
                                .or_else(|| obj.get("kind"))
                                .or_else(|| obj.get("label"))
                                .and_then(|v| v.as_str())
                                .map(String::from)
                                .unwrap_or_else(|| field_name.clone());

                            let ri =
                                graph.add_node(Node::new(eid, Some(elabel), element.clone()));
                            graph.add_edge(Edge::new(root_idx, ri, field_name.clone()));
                        }
                    }
                }
            }
            Value::Object(_) => {
                let obj = field_value.as_object().unwrap();
                let is_pure_wrapper = obj.values().all(|v| v.is_array() || v.is_object());

                if is_pure_wrapper {
                    // Unwrap pure wrappers — process their children at root level
                    for (inner_key, inner_value) in obj {
                        if let Some(inner_arr) = inner_value.as_array() {
                            if inner_arr.is_empty() {
                                continue;
                            }
                            if let Some(first) = inner_arr.first() {
                                if !first.is_object() {
                                    continue;
                                }
                                for (idx, element) in inner_arr.iter().enumerate() {
                                    if let Value::Object(elem_obj) = element {
                                        let eid = elem_obj
                                            .get("id")
                                            .or_else(|| elem_obj.get("_id"))
                                            .and_then(|v| v.as_str())
                                            .map(String::from)
                                            .unwrap_or_else(|| {
                                                format!("{}-{}", inner_key, idx)
                                            });
                                        let elabel = elem_obj
                                            .get("type")
                                            .or_else(|| elem_obj.get("kind"))
                                            .or_else(|| elem_obj.get("label"))
                                            .and_then(|v| v.as_str())
                                            .map(String::from)
                                            .unwrap_or_else(|| inner_key.clone());
                                        let ri = graph.add_node(Node::new(
                                            eid,
                                            Some(elabel),
                                            element.clone(),
                                        ));
                                        graph.add_edge(Edge::new(
                                            root_idx,
                                            ri,
                                            inner_key.clone(),
                                        ));
                                    }
                                }
                            }
                        } else if let Some(inner_obj) = inner_value.as_object() {
                            let inner_pure = inner_obj
                                .values()
                                .all(|v| v.is_array() || v.is_object());
                            if !inner_pure {
                                // Leaf object → child node
                                let eid = inner_obj
                                    .get("id")
                                    .or_else(|| inner_obj.get("_id"))
                                    .and_then(|v| v.as_str())
                                    .map(String::from)
                                    .unwrap_or_else(|| inner_key.clone());
                                let elabel = inner_obj
                                    .get("type")
                                    .or_else(|| inner_obj.get("kind"))
                                    .or_else(|| inner_obj.get("label"))
                                    .and_then(|v| v.as_str())
                                    .map(String::from)
                                    .unwrap_or_else(|| inner_key.clone());
                                let ri = graph.add_node(Node::new(
                                    eid,
                                    Some(elabel),
                                    inner_value.clone(),
                                ));
                                graph.add_edge(Edge::new(
                                    root_idx,
                                    ri,
                                    inner_key.clone(),
                                ));
                            }
                        }
                    }
                } else {
                    // Leaf object → single child node
                    let eid = obj
                        .get("id")
                        .or_else(|| obj.get("_id"))
                        .and_then(|v| v.as_str())
                        .map(String::from)
                        .unwrap_or_else(|| field_name.clone());

                    let elabel = obj
                        .get("type")
                        .or_else(|| obj.get("kind"))
                        .or_else(|| obj.get("label"))
                        .and_then(|v| v.as_str())
                        .map(String::from)
                        .unwrap_or_else(|| field_name.clone());

                    let ri = graph.add_node(Node::new(
                        eid,
                        Some(elabel),
                        field_value.clone(),
                    ));
                    graph.add_edge(Edge::new(root_idx, ri, field_name.clone()));
                }
            }
            _ => {}
        }
    }

    // Second pass: add inter-child edges from relation fields
    let mut inter_edges: Vec<(usize, usize, String)> = Vec::new();
    for (child_idx, child_node) in graph.nodes.iter().enumerate() {
        if child_idx == root_idx {
            continue;
        }
        if let Value::Object(data) = &child_node.data {
            for (field_name, field_value) in data {
                if let Some(id_array) = field_value.as_array() {
                    for id_val in id_array {
                        if let Some(to_id) = id_val.as_str() {
                            if let Some(to_idx) = graph.get_node_index(to_id) {
                                inter_edges
                                    .push((child_idx, to_idx, field_name.clone()));
                            }
                        }
                    }
                }
            }
        }
    }
    for (from, to, rel_type) in inter_edges {
        graph.add_edge(Edge::new(from, to, rel_type));
    }

    Ok(graph)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_json_storage_from_str() {
        let json_str = r#"{
            "users": [
                { "id": "1", "role": "admin" },
                { "id": "2", "role": "user" }
            ]
        }"#;

        let storage = JsonStorage::from_str(json_str).unwrap();
        let graph = storage.load_graph_sync().unwrap();
        assert_eq!(graph.nodes.len(), 3); // Root + 2 users
    }

    #[test]
    fn test_json_storage_metadata() {
        let data = json!({"users": []});
        let storage = JsonStorage::from_value(data);
        let metadata = storage.metadata();

        assert_eq!(metadata.name, "json");
        assert!(metadata.features.contains(&StorageFeature::ConcurrentReads));
    }

    #[test]
    fn test_root_object_strips_array_fields() {
        let data = json!({
            "id": "US123",
            "title": "Test Patent",
            "claims": [
                { "id": "c1", "number": "1", "text": "Claim 1" }
            ]
        });

        let graph = build_graph_from_root_object(&data, "Patent").unwrap();
        assert_eq!(graph.nodes.len(), 2); // 1 Patent + 1 Claim

        // Root node should NOT have "claims" in its data
        let root = &graph.nodes[0];
        assert!(root.get_property("claims").is_none());
        assert_eq!(
            root.get_property_as_string("title"),
            Some("Test Patent".to_string())
        );
    }

    #[test]
    fn test_root_object_relationship_type_from_field_name() {
        let data = json!({
            "id": "doc-1",
            "sections": [
                { "id": "s1", "heading": "Intro" }
            ],
            "authors": [
                { "id": "a1", "name": "Alice" }
            ]
        });

        let graph = build_graph_from_root_object(&data, "Document").unwrap();
        assert_eq!(graph.edges.len(), 2);

        let rel_types: Vec<&str> =
            graph.edges.iter().map(|e| e.rel_type.as_str()).collect();
        assert!(rel_types.contains(&"sections"));
        assert!(rel_types.contains(&"authors"));
    }

    #[test]
    fn test_root_object_with_object_values() {
        let data = json!({
            "object1": { "prop1": "x", "prop2": "y" },
            "object2": { "prop3": "z" }
        });

        let graph = build_graph_from_root_object(&data, "Root").unwrap();
        assert_eq!(graph.nodes.len(), 3); // Root + 2 objects

        let labels: Vec<&str> = graph.nodes[1..]
            .iter()
            .map(|n| n.label.as_deref().unwrap())
            .collect();
        assert!(labels.contains(&"object1"));
        assert!(labels.contains(&"object2"));

        let rel_types: Vec<&str> =
            graph.edges.iter().map(|e| e.rel_type.as_str()).collect();
        assert!(rel_types.contains(&"object1"));
        assert!(rel_types.contains(&"object2"));
    }

    #[test]
    fn test_root_object_strips_object_values_from_data() {
        let data = json!({
            "id": "r1",
            "name": "root",
            "child": { "id": "c1", "value": "hello" }
        });

        let graph = build_graph_from_root_object(&data, "Root").unwrap();
        assert_eq!(graph.nodes.len(), 2);

        let root = &graph.nodes[0];
        assert!(root.get_property("child").is_none());
        assert_eq!(
            root.get_property_as_string("name"),
            Some("root".to_string())
        );
    }
}
