use serde::{Deserialize, Serialize};

/// Configuration for nested node arrays within a root object.
///
/// When a root object contains arrays of related nodes (e.g., a Patent with claims array),
/// this configuration defines how to create relationships from the root to those nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelatedNodeArray {
    /// The field name containing the array of related nodes
    pub field_name: String,
    /// The relationship type to create (e.g., "HAS_CLAIM")
    pub relationship_type: String,
    /// The ID field within the related node objects
    pub id_field: String,
    /// Optional label field within the related node objects
    pub label_field: Option<String>,
}

impl RelatedNodeArray {
    /// Create a new RelatedNodeArray configuration.
    pub fn new(
        field_name: impl Into<String>,
        relationship_type: impl Into<String>,
        id_field: impl Into<String>,
        label_field: Option<String>,
    ) -> Self {
        Self {
            field_name: field_name.into(),
            relationship_type: relationship_type.into(),
            id_field: id_field.into(),
            label_field,
        }
    }
}

/// Configuration for treating a root JSON object as a node.
///
/// When JSON data has a root object that should be treated as a node (rather than
/// an array of nodes), this configuration defines how to create that node and
/// its relationships to nested array elements.
///
/// # Example
///
/// ```rust
/// use cypher_rs::config::RootObjectConfig;
///
/// let config = RootObjectConfig {
///     primary_label: "Patent".to_string(),
///     id_field: "id".to_string(),
///     label_field: None,
///     related_node_arrays: vec![],
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RootObjectConfig {
    /// The primary label for the root node (e.g., "Patent", "Root")
    pub primary_label: String,
    /// The field to use as the ID for the root node
    pub id_field: String,
    /// Optional field to use as the label for the root node
    pub label_field: Option<String>,
    /// Arrays within the root object that contain related nodes
    pub related_node_arrays: Vec<RelatedNodeArray>,
}

impl RootObjectConfig {
    /// Create a new RootObjectConfig with the given settings.
    pub fn new(
        primary_label: impl Into<String>,
        id_field: impl Into<String>,
        label_field: Option<String>,
        related_node_arrays: Vec<RelatedNodeArray>,
    ) -> Self {
        Self {
            primary_label: primary_label.into(),
            id_field: id_field.into(),
            label_field,
            related_node_arrays,
        }
    }

    /// Create a minimal RootObjectConfig with only required fields.
    pub fn minimal(primary_label: impl Into<String>, id_field: impl Into<String>) -> Self {
        Self {
            primary_label: primary_label.into(),
            id_field: id_field.into(),
            label_field: None,
            related_node_arrays: Vec::new(),
        }
    }
}

/// Configuration for mapping JSON data to a graph structure.
///
/// # Example
///
/// ```rust
/// use cypher_rs::GraphConfig;
///
/// let config = GraphConfig {
///     node_path: "users".to_string(),
///     id_field: "id".to_string(),
///     label_field: Some("role".to_string()),
///     relation_fields: vec!["friends".to_string()],
///     root_object_config: None,
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphConfig {
    /// JSON path to the array of nodes (e.g., "data.users.*" or "users")
    pub node_path: String,

    /// Field name for the node ID
    pub id_field: String,

    /// Optional field name for the node label
    pub label_field: Option<String>,

    /// Field names that contain arrays of related node IDs
    pub relation_fields: Vec<String>,

    /// Optional configuration for treating root object as a node
    #[serde(skip_serializing_if = "Option::is_none")]
    pub root_object_config: Option<RootObjectConfig>,
}

impl GraphConfig {
    /// Create a new GraphConfig with the given settings.
    pub fn new(
        node_path: impl Into<String>,
        id_field: impl Into<String>,
        label_field: Option<String>,
        relation_fields: Vec<String>,
    ) -> Self {
        Self {
            node_path: node_path.into(),
            id_field: id_field.into(),
            label_field,
            relation_fields,
            root_object_config: None,
        }
    }

    /// Create a minimal GraphConfig with only required fields.
    pub fn minimal(node_path: impl Into<String>, id_field: impl Into<String>) -> Self {
        Self {
            node_path: node_path.into(),
            id_field: id_field.into(),
            label_field: None,
            relation_fields: Vec::new(),
            root_object_config: None,
        }
    }

    /// Create a GraphConfig for root object as node mode.
    pub fn root_object(primary_label: impl Into<String>, id_field: impl Into<String>) -> Self {
        let id_field = id_field.into();
        Self {
            node_path: String::new(), // Not used in root object mode
            id_field: id_field.clone(),
            label_field: None,
            relation_fields: Vec::new(),
            root_object_config: Some(RootObjectConfig::minimal(primary_label, id_field)),
        }
    }

    /// Set the root object configuration.
    pub fn with_root_object_config(mut self, config: RootObjectConfig) -> Self {
        self.root_object_config = Some(config);
        self
    }
}

impl Default for GraphConfig {
    fn default() -> Self {
        Self {
            node_path: "nodes".to_string(),
            id_field: "id".to_string(),
            label_field: None,
            relation_fields: Vec::new(),
            root_object_config: None,
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
        assert!(config.label_field.is_none());
        assert!(config.relation_fields.is_empty());
        assert!(config.root_object_config.is_none());
    }

    #[test]
    fn test_new_config() {
        let config = GraphConfig::new(
            "users",
            "id",
            Some("role".to_string()),
            vec!["friends".to_string()],
        );
        assert_eq!(config.node_path, "users");
        assert_eq!(config.id_field, "id");
        assert_eq!(config.label_field, Some("role".to_string()));
        assert_eq!(config.relation_fields, vec!["friends".to_string()]);
        assert!(config.root_object_config.is_none());
    }

    #[test]
    fn test_minimal_config() {
        let config = GraphConfig::minimal("users", "id");
        assert_eq!(config.node_path, "users");
        assert_eq!(config.id_field, "id");
        assert!(config.label_field.is_none());
        assert!(config.relation_fields.is_empty());
        assert!(config.root_object_config.is_none());
    }

    #[test]
    fn test_root_object_config() {
        let config = GraphConfig::root_object("Patent", "id");
        assert!(config.root_object_config.is_some());
        let root_config = config.root_object_config.as_ref().unwrap();
        assert_eq!(root_config.primary_label, "Patent");
        assert_eq!(root_config.id_field, "id");
    }

    #[test]
    fn test_related_node_array() {
        let related = RelatedNodeArray::new("claims", "HAS_CLAIM", "id", None);
        assert_eq!(related.field_name, "claims");
        assert_eq!(related.relationship_type, "HAS_CLAIM");
        assert_eq!(related.id_field, "id");
        assert!(related.label_field.is_none());
    }
}
