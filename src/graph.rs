use serde_json::Value;
use std::collections::HashMap;

/// A graph structure containing nodes and edges.
#[derive(Debug, Clone)]
pub struct Graph {
    pub nodes: Vec<Node>,
    pub edges: Vec<Edge>,
    /// Maps node IDs to their index in the nodes vector
    pub id_map: HashMap<String, usize>,
}

impl Graph {
    /// Create a new empty graph.
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            edges: Vec::new(),
            id_map: HashMap::new(),
        }
    }

    /// Add a node to the graph.
    pub fn add_node(&mut self, node: Node) -> usize {
        let idx = self.nodes.len();
        self.id_map.insert(node.id.clone(), idx);
        self.nodes.push(node);
        idx
    }

    /// Get a node by its ID.
    pub fn get_node(&self, id: &str) -> Option<&Node> {
        self.id_map.get(id).map(|&idx| &self.nodes[idx])
    }

    /// Get a node index by its ID.
    pub fn get_node_index(&self, id: &str) -> Option<usize> {
        self.id_map.get(id).copied()
    }

    /// Add an edge to the graph.
    pub fn add_edge(&mut self, edge: Edge) {
        self.edges.push(edge);
    }

    /// Get all edges from a given node index.
    pub fn get_outgoing_edges(&self, from_idx: usize) -> Vec<&Edge> {
        self.edges.iter().filter(|e| e.from == from_idx).collect()
    }

    /// Get all edges to a given node index.
    pub fn get_incoming_edges(&self, to_idx: usize) -> Vec<&Edge> {
        self.edges.iter().filter(|e| e.to == to_idx).collect()
    }
}

impl Default for Graph {
    fn default() -> Self {
        Self::new()
    }
}

/// A node in the graph.
#[derive(Debug, Clone)]
pub struct Node {
    pub id: String,
    pub label: Option<String>,
    pub data: Value,
}

impl Node {
    /// Create a new node.
    pub fn new(id: impl Into<String>, label: Option<String>, data: Value) -> Self {
        Self {
            id: id.into(),
            label,
            data,
        }
    }

    /// Get a property value from the node's data.
    pub fn get_property(&self, key: &str) -> Option<&Value> {
        self.data.get(key)
    }

    /// Get a property as a string.
    pub fn get_property_as_string(&self, key: &str) -> Option<String> {
        self.data.get(key).and_then(|v| match v {
            Value::String(s) => Some(s.clone()),
            Value::Number(n) => Some(n.to_string()),
            Value::Bool(b) => Some(b.to_string()),
            Value::Null => None,
            _ => None,
        })
    }

    /// Get a property as an i64.
    pub fn get_property_as_i64(&self, key: &str) -> Option<i64> {
        self.data.get(key).and_then(|v| match v {
            Value::Number(n) => n.as_i64(),
            _ => None,
        })
    }
}

/// An edge in the graph.
#[derive(Debug, Clone)]
pub struct Edge {
    pub from: usize,
    pub to: usize,
    pub rel_type: String,
}

impl Edge {
    /// Create a new edge.
    pub fn new(from: usize, to: usize, rel_type: impl Into<String>) -> Self {
        Self {
            from,
            to,
            rel_type: rel_type.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_graph_creation() {
        let graph = Graph::new();
        assert_eq!(graph.nodes.len(), 0);
        assert_eq!(graph.edges.len(), 0);
    }

    #[test]
    fn test_add_node() {
        let mut graph = Graph::new();
        let node = Node::new(
            "1".to_string(),
            Some("User".to_string()),
            json!({"name": "Alice"}),
        );
        let idx = graph.add_node(node);
        assert_eq!(idx, 0);
        assert_eq!(graph.nodes.len(), 1);
    }

    #[test]
    fn test_get_node() {
        let mut graph = Graph::new();
        let node = Node::new(
            "1".to_string(),
            Some("User".to_string()),
            json!({"name": "Alice"}),
        );
        graph.add_node(node);
        assert!(graph.get_node("1").is_some());
        assert!(graph.get_node("2").is_none());
    }

    #[test]
    fn test_add_edge() {
        let mut graph = Graph::new();
        let node1 = Node::new("1".to_string(), Some("User".to_string()), json!({}));
        let node2 = Node::new("2".to_string(), Some("User".to_string()), json!({}));
        graph.add_node(node1);
        graph.add_node(node2);
        graph.add_edge(Edge::new(0, 1, "knows".to_string()));
        assert_eq!(graph.edges.len(), 1);
    }

    #[test]
    fn test_node_get_property() {
        let node = Node::new(
            "1".to_string(),
            Some("User".to_string()),
            json!({"name": "Alice", "age": 30}),
        );
        assert_eq!(
            node.get_property_as_string("name"),
            Some("Alice".to_string())
        );
        assert_eq!(node.get_property_as_i64("age"), Some(30));
        assert!(node.get_property_as_string("unknown").is_none());
    }
}
