use serde::{Deserialize, Serialize};

/// Configuration for mapping JSON data to a graph structure.
///
/// The node label is derived from the array key in the JSON (i.e., the last
/// segment of `node_path`). For example, a `node_path` of `"users"` produces
/// nodes with label `:users`, and `"data.Patent"` produces `:Patent`.
///
/// # Example
///
/// ```rust
/// use cypher_rs::GraphConfig;
///
/// let config = GraphConfig {
///     node_path: "users".to_string(),
///     id_field: "id".to_string(),
///     relation_fields: vec!["friends".to_string()],
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphConfig {
    /// JSON path to the array of nodes (e.g., "data.users" or "Patent").
    /// The last segment is used as the node label.
    pub node_path: String,

    /// Field name for the node ID
    pub id_field: String,

    /// Field names that contain arrays of related node IDs
    pub relation_fields: Vec<String>,
}

impl GraphConfig {
    /// Create a new GraphConfig with the given settings.
    pub fn new(
        node_path: impl Into<String>,
        id_field: impl Into<String>,
        relation_fields: Vec<String>,
    ) -> Self {
        Self {
            node_path: node_path.into(),
            id_field: id_field.into(),
            relation_fields,
        }
    }

    /// Create a minimal GraphConfig with only required fields.
    pub fn minimal(node_path: impl Into<String>, id_field: impl Into<String>) -> Self {
        Self {
            node_path: node_path.into(),
            id_field: id_field.into(),
            relation_fields: Vec::new(),
        }
    }

    /// Derive the node label from the last segment of `node_path`.
    ///
    /// For `"users"` → `"users"`, for `"data.Patent"` → `"Patent"`.
    pub fn label(&self) -> String {
        self.node_path
            .rsplit('.')
            .next()
            .unwrap_or(&self.node_path)
            .to_string()
    }
}

impl Default for GraphConfig {
    fn default() -> Self {
        Self {
            node_path: "nodes".to_string(),
            id_field: "id".to_string(),
            relation_fields: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = GraphConfig::default();
        assert_eq!(config.node_path, "nodes");
        assert_eq!(config.id_field, "id");
        assert!(config.relation_fields.is_empty());
    }

    #[test]
    fn test_new_config() {
        let config = GraphConfig::new("users", "id", vec!["friends".to_string()]);
        assert_eq!(config.node_path, "users");
        assert_eq!(config.id_field, "id");
        assert_eq!(config.relation_fields, vec!["friends".to_string()]);
    }

    #[test]
    fn test_minimal_config() {
        let config = GraphConfig::minimal("users", "id");
        assert_eq!(config.node_path, "users");
        assert_eq!(config.id_field, "id");
        assert!(config.relation_fields.is_empty());
    }

    #[test]
    fn test_label() {
        let config = GraphConfig::minimal("users", "id");
        assert_eq!(config.label(), "users");

        let config = GraphConfig::minimal("data.Patent", "id");
        assert_eq!(config.label(), "Patent");
    }
}
