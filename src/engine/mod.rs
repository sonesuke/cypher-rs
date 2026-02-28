//! Cypher query execution engine.
//!
//! This module provides the core query execution functionality for the Cypher-RS library.
//! It is organized into submodules for better separation of concerns:

pub mod storage;
pub mod functions;
pub mod executor;

use crate::parser;
use crate::graph::Graph;
pub use executor::{EntityId, QueryExecutor};

use serde_json::Value;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum EngineError {
    #[error("Parse error: {0}")]
    ParseError(#[from] anyhow::Error),

    #[error("Execution error: {0}")]
    ExecutionError(String),

    #[error("Invalid JSON structure: {0}")]
    InvalidJson(String),
}

pub type Result<T> = std::result::Result<T, EngineError>;

/// Result of a Cypher query execution.
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Value>,
}

impl QueryResult {
    pub fn new(columns: Vec<String>, rows: Vec<Value>) -> Self {
        Self { columns, rows }
    }

    /// Get the result as a JSON array of objects.
    pub fn as_json_array(&self) -> Value {
        let arr: Vec<Value> = self
            .rows
            .iter()
            .map(|row| {
                let mut obj = serde_json::Map::new();
                for col in &self.columns {
                    if let Some(row_obj) = row.as_object() {
                        if let Some(val) = row_obj.get(col) {
                            obj.insert(col.clone(), val.clone());
                        }
                    }
                }
                Value::Object(obj)
            })
            .collect();
        Value::Array(arr)
    }

    /// Get a single aggregate result (for queries like COUNT, SUM).
    pub fn get_single_value(&self) -> Option<&Value> {
        if self.rows.len() == 1 && self.columns.len() == 1 {
            self.rows[0].get(&self.columns[0])
        } else {
            None
        }
    }
}

/// Execute a Cypher query against a graph.
///
/// This is a convenience function that parses and executes a query.
///
/// # Example
///
/// ```rust
/// use cypher_rs::engine::execute;
/// use cypher_rs::graph::Graph;
///
/// let graph = Graph::new(); // Your graph here
/// let result = execute("MATCH (n) RETURN COUNT(n)", &graph).unwrap();
/// ```
pub fn execute(query: &str, graph: &Graph) -> Result<QueryResult> {
    let ast_query = parser::parse_query(query)?;
    QueryExecutor::execute(&ast_query, graph)
}

// Re-exports for convenience
pub use storage::{Storage, SyncStorage, JsonStorage, MemoryStorage, MemoryStorageBuilder};
pub use storage::{StorageError, StorageResult, StorageFeature, StorageMetadata};
pub use functions::{EvalContext, ExpressionContext, AggregateEvaluator, FunctionError, FunctionResult};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::GraphConfig;
    use crate::graph::Node;
    use serde_json::json;

    fn create_test_graph() -> Graph {
        let mut graph = Graph::new();
        graph.add_node(Node::new(
            "1".to_string(),
            Some("admin".to_string()),
            json!({"id": "1", "role": "admin", "age": 30}),
        ));
        graph.add_node(Node::new(
            "2".to_string(),
            Some("user".to_string()),
            json!({"id": "2", "role": "user", "age": 25}),
        ));
        graph.add_node(Node::new(
            "3".to_string(),
            Some("admin".to_string()),
            json!({"id": "3", "role": "admin", "age": 35}),
        ));

        graph.add_edge(crate::graph::Edge::new(0, 1, "knows".to_string()));
        graph.add_edge(crate::graph::Edge::new(1, 2, "knows".to_string()));

        graph
    }

    #[test]
    fn test_execute_convenience() {
        let graph = create_test_graph();
        let result = execute("MATCH (n) RETURN COUNT(n)", &graph).unwrap();
        assert_eq!(result.get_single_value().unwrap().as_i64(), Some(3));
    }

    #[test]
    fn test_storage_integration() {
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
        };

        let graph = storage.load_graph_sync(&config).unwrap();
        let result = execute("MATCH (u) RETURN COUNT(u)", &graph).unwrap();
        assert_eq!(result.get_single_value().unwrap().as_i64(), Some(2));
    }

    #[test]
    fn test_memory_storage() {
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

        let storage = MemoryStorage::from_graph(graph.clone());
        let loaded_graph = storage.load_graph_sync(&GraphConfig::default()).unwrap();
        assert_eq!(loaded_graph.nodes.len(), 2);

        let result = execute("MATCH (n) RETURN COUNT(n)", &graph).unwrap();
        assert_eq!(result.get_single_value().unwrap().as_i64(), Some(2));
    }

    #[test]
    fn test_query_result_as_json() {
        let graph = create_test_graph();
        let result = execute("MATCH (n:admin) RETURN n.id", &graph).unwrap();
        let json_array = result.as_json_array();

        assert!(json_array.is_array());
        assert_eq!(json_array.as_array().unwrap().len(), 2);
    }
}
