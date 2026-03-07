use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fmt;

use crate::config::{GraphConfig, RelatedNodeArray, RootObjectConfig};

/// Result type for schema detection.
pub type SchemaResult<T> = std::result::Result<T, SchemaError>;

/// Errors that can occur during schema detection.
#[derive(Debug, thiserror::Error)]
pub enum SchemaError {
    #[error("Invalid JSON structure: {0}")]
    InvalidJson(String),

    #[error("No suitable array found in JSON")]
    NoArrayFound,

    #[error("Empty JSON object")]
    EmptyJson,
}

/// Detected field information for a node type.
#[derive(Debug, Clone)]
pub struct NodeFieldInfo {
    /// The field name
    pub name: String,
    /// Inferred field type
    pub field_type: FieldType,
    /// Whether this field could be an ID field
    pub is_id_candidate: bool,
    /// Whether this field could be a label field
    pub is_label_candidate: bool,
    /// Whether this field could be a relation field (contains array of IDs)
    pub is_relation_candidate: bool,
}

/// Field type classification.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FieldType {
    String,
    Number,
    Boolean,
    Array,
    Object,
    Null,
}

impl fmt::Display for FieldType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FieldType::String => write!(f, "STRING"),
            FieldType::Number => write!(f, "NUMBER"),
            FieldType::Boolean => write!(f, "BOOLEAN"),
            FieldType::Array => write!(f, "ARRAY"),
            FieldType::Object => write!(f, "OBJECT"),
            FieldType::Null => write!(f, "NULL"),
        }
    }
}

/// Detected schema for a root object that should be treated as a node.
#[derive(Debug, Clone)]
pub struct RootObjectSchema {
    /// Path to the root object (typically empty string for root)
    pub path: String,
    /// Detected fields in the root object
    pub fields: Vec<NodeFieldInfo>,
    /// Recommended ID field for the root node
    pub recommended_id_field: Option<String>,
    /// Recommended label field for the root node
    pub recommended_label_field: Option<String>,
    /// Detected nested arrays that contain related nodes
    pub related_node_arrays: Vec<RelatedNodeArraySchema>,
}

/// Schema for a nested array within a root object.
#[derive(Debug, Clone)]
pub struct RelatedNodeArraySchema {
    /// Field name of the array
    pub field_name: String,
    /// Inferred relationship type (e.g., "claims" -> "HAS_CLAIM")
    pub relationship_type: String,
    /// Number of elements in the array
    pub element_count: usize,
    /// Recommended ID field for elements in this array
    pub recommended_id_field: Option<String>,
    /// Recommended label field for elements in this array
    pub recommended_label_field: Option<String>,
}

/// Detected schema for a JSON array.
#[derive(Debug, Clone)]
pub struct ArraySchema {
    /// Path to this array (e.g., "users" or "data.users")
    pub path: String,
    /// Number of elements in the array
    pub element_count: usize,
    /// Detected fields in the array elements
    pub fields: Vec<NodeFieldInfo>,
    /// Recommended ID field
    pub recommended_id_field: Option<String>,
    /// Recommended label field
    pub recommended_label_field: Option<String>,
    /// Fields that appear to be relations (arrays of IDs)
    pub recommended_relation_fields: Vec<String>,
}

/// Schema detection result for a JSON document.
#[derive(Debug, Clone)]
pub struct SchemaDetection {
    /// All detected array schemas
    pub array_schemas: Vec<ArraySchema>,
    /// Primary recommendation (the most suitable array for graph construction)
    pub primary_recommendation: Option<ArraySchema>,
    /// Root object schema (if root should be treated as a node)
    pub root_object_schema: Option<RootObjectSchema>,
}

impl SchemaDetection {
    /// Create a new schema detection result.
    pub fn new(
        array_schemas: Vec<ArraySchema>,
        primary_recommendation: Option<ArraySchema>,
        root_object_schema: Option<RootObjectSchema>,
    ) -> Self {
        Self {
            array_schemas,
            primary_recommendation,
            root_object_schema,
        }
    }

    /// Get the best GraphConfig based on detected schema.
    ///
    /// If a root object schema is detected, it will be preferred over array schemas.
    pub fn to_graph_config(&self) -> Option<GraphConfig> {
        // Prefer root object schema if present
        if let Some(root_schema) = &self.root_object_schema {
            let id_field = root_schema.recommended_id_field.clone()?;
            let label_field = root_schema.recommended_label_field.clone();

            // Convert related node array schemas to RelatedNodeArray configs
            let related_node_arrays: Vec<RelatedNodeArray> = root_schema
                .related_node_arrays
                .iter()
                .map(|arr| {
                    RelatedNodeArray::new(
                        arr.field_name.clone(),
                        arr.relationship_type.clone(),
                        arr.recommended_id_field
                            .clone()
                            .unwrap_or_else(|| "id".to_string()),
                        arr.recommended_label_field.clone(),
                    )
                })
                .collect();

            let root_config = RootObjectConfig::new(
                "Root", // Default label, can be overridden by users
                id_field,
                label_field,
                related_node_arrays,
            );

            return Some(GraphConfig {
                node_path: String::new(),
                id_field: root_config.id_field.clone(),
                label_field: root_config.label_field.clone(),
                relation_fields: Vec::new(),
                root_object_config: Some(root_config),
            });
        }

        // Fall back to array-based schema
        let schema = &self.primary_recommendation.as_ref()?;
        let id_field = schema.recommended_id_field.clone()?;
        let label_field = schema.recommended_label_field.clone();

        Some(GraphConfig {
            node_path: schema.path.clone(),
            id_field,
            label_field,
            relation_fields: schema.recommended_relation_fields.clone(),
            root_object_config: None,
        })
    }

    /// Get all possible GraphConfigs from detected schemas.
    pub fn to_graph_configs(&self) -> Vec<GraphConfig> {
        self.array_schemas
            .iter()
            .filter_map(|schema| {
                let id_field = schema.recommended_id_field.clone()?;
                Some(GraphConfig {
                    node_path: schema.path.clone(),
                    id_field,
                    label_field: schema.recommended_label_field.clone(),
                    relation_fields: schema.recommended_relation_fields.clone(),
                    root_object_config: None,
                })
            })
            .collect()
    }

    /// Generate a Neo4j-style schema representation as a string.
    ///
    /// Returns a visual representation of the graph schema similar to Neo4j's
    /// schema visualization or `CALL db.schema()` output.
    ///
    /// # Example
    ///
    /// ```rust
    /// use cypher_rs::schema::SchemaAnalyzer;
    /// use serde_json::json;
    ///
    /// let data = json!({
    ///     "users": [
    ///         { "id": "1", "role": "admin", "name": "Alice", "friends": ["2"] }
    ///     ]
    /// });
    ///
    /// let schema = SchemaAnalyzer::analyze(&data).unwrap();
    /// let neo4j_schema = schema.to_neo4j_schema();
    /// println!("{}", neo4j_schema);
    /// ```
    ///
    /// # Output Format
    ///
    /// ```text
    /// Graph Schema
    /// =============
    ///
    /// Node Types:
    ///   (:admin {id: STRING, name: STRING, friends: ARRAY})
    ///   (:user {id: STRING, name: STRING, friends: ARRAY})
    ///
    /// Relationship Types:
    ///   (:admin)-[:friends]->(:admin)
    ///   (:admin)-[:friends]->(:user)
    /// ```
    pub fn to_neo4j_schema(&self) -> String {
        let mut output = String::new();

        // Find the primary schema
        let primary = self
            .primary_recommendation
            .as_ref()
            .or_else(|| self.array_schemas.first());

        let Some(schema) = primary else {
            return "No schema detected".to_string();
        };

        output.push_str("Graph Schema\n");
        output.push_str("============\n\n");

        // Collect unique labels
        let mut labels: Vec<String> = Vec::new();
        let mut properties_by_label: HashMap<String, Vec<String>> = HashMap::new();
        let mut relations: Vec<(String, String, String)> = Vec::new(); // (from_label, rel_type, to_label)

        // Infer labels from data (use the label field values)
        // For now, we'll use the path as the node type if no label field
        let node_type_prefix = schema.path.split('.').next_back().unwrap_or(&schema.path);
        let has_label_field = schema.recommended_label_field.is_some();

        // Collect properties by analyzing the fields
        let all_properties: Vec<String> = schema
            .fields
            .iter()
            .filter(|f| !f.is_relation_candidate)
            .map(|f| {
                let type_str = match f.field_type {
                    FieldType::String => "STRING",
                    FieldType::Number => "NUMBER",
                    FieldType::Boolean => "BOOLEAN",
                    FieldType::Array => "ARRAY",
                    FieldType::Object => "OBJECT",
                    FieldType::Null => "NULL",
                };
                format!("{}: {}", f.name, type_str)
            })
            .collect();

        // Determine labels
        if has_label_field {
            // If there's a label field, we can't determine actual label values without data
            // So we'll use the field name as the label type
            let label_field = schema.recommended_label_field.as_ref().unwrap();
            labels.push(format!("{} (by {})", node_type_prefix, label_field));
            properties_by_label.insert(node_type_prefix.to_string(), all_properties);
        } else {
            labels.push(node_type_prefix.to_string());
            properties_by_label.insert(node_type_prefix.to_string(), all_properties);
        }

        // Build relationship patterns
        for rel_field in &schema.recommended_relation_fields {
            // Self-referential relationship (most common case)
            let rel_type = rel_field;
            relations.push((
                node_type_prefix.to_string(),
                rel_type.clone(),
                node_type_prefix.to_string(),
            ));
        }

        // Output Node Types
        output.push_str("Node Types:\n");
        for label in &labels {
            output.push_str(&format!("  (:{})\n", label));
        }
        output.push('\n');

        // Output Properties (grouped by node type)
        output.push_str("Properties:\n");
        for (node_type, props) in &properties_by_label {
            output.push_str(&format!("  :{} {{{}}}\n", node_type, props.join(", ")));
        }
        output.push('\n');

        // Output Relationship Types
        if !relations.is_empty() {
            output.push_str("Relationship Types:\n");
            for (from, rel, to) in &relations {
                output.push_str(&format!("  (:{})-[:{}]->(:{})\n", from, rel, to));
            }
        }

        output
    }

    /// Generate a simplified Cypher-style pattern representation.
    ///
    /// This is a compact single-line representation of the schema pattern.
    ///
    /// # Example Output
    ///
    /// ```text
    /// (:User {id, name, role})-[:FRIENDS]->(:User)
    /// ```
    pub fn to_pattern(&self) -> String {
        let primary = self
            .primary_recommendation
            .as_ref()
            .or_else(|| self.array_schemas.first());

        let Some(schema) = primary else {
            return "()".to_string();
        };

        let node_type = schema.path.split('.').next_back().unwrap_or(&schema.path);
        let id_field = schema.recommended_id_field.as_deref().unwrap_or("id");

        // Build properties list (exclude relations)
        let properties: Vec<String> = schema
            .fields
            .iter()
            .filter(|f| !f.is_relation_candidate && f.name != id_field)
            .map(|f| f.name.clone())
            .collect();

        // Build node pattern
        let node_pattern = if properties.is_empty() {
            format!("(:{{{}:{}}})", node_type, id_field)
        } else {
            format!(
                "(:{} {{{}, {}}})",
                node_type,
                id_field,
                properties.join(", ")
            )
        };

        // Build relationship patterns
        let mut patterns = Vec::new();
        for rel_field in &schema.recommended_relation_fields {
            patterns.push(format!(
                "{}-[:{}]->{}",
                node_pattern, rel_field, node_pattern
            ));
        }

        if patterns.is_empty() {
            node_pattern
        } else {
            patterns.join(" | ")
        }
    }
}

/// Infer a relationship type from a field name.
///
/// All nested arrays are treated as child nodes with a HAS_CHILD relationship.
///
/// # Examples
///
/// ```rust
/// use cypher_rs::schema::infer_relationship_type;
///
/// assert_eq!(infer_relationship_type("claims"), "HAS_CHILD");
/// assert_eq!(infer_relationship_type("description_paragraphs"), "HAS_CHILD");
/// assert_eq!(infer_relationship_type("users"), "HAS_CHILD");
/// ```
pub fn infer_relationship_type(_field_name: &str) -> String {
    "HAS_CHILD".to_string()
}

/// Schema analyzer for JSON documents.
pub struct SchemaAnalyzer;

impl SchemaAnalyzer {
    /// Analyze a JSON document and detect its schema.
    ///
    /// # Example
    ///
    /// ```rust
    /// use cypher_rs::schema::SchemaAnalyzer;
    /// use serde_json::json;
    ///
    /// let data = json!({
    ///     "users": [
    ///         { "id": "1", "role": "admin", "age": 30, "friends": ["2"] }
    ///     ]
    /// });
    ///
    /// let schema = SchemaAnalyzer::analyze(&data).unwrap();
    /// let config = schema.to_graph_config().unwrap();
    /// ```
    pub fn analyze(data: &Value) -> SchemaResult<SchemaDetection> {
        // First, check if the root is an object that should be treated as a node
        if let Value::Object(root_obj) = data {
            // Check for root object schema (root has arrays of related nodes)
            let root_object_schema = detect_root_object_node(root_obj);

            // If we detected a root object schema, use it
            if let Some(root_schema) = root_object_schema {
                return Ok(SchemaDetection::new(Vec::new(), None, Some(root_schema)));
            }
        }

        // Fall back to array-based detection
        let mut array_schemas = Vec::new();
        find_arrays(data, "", &mut array_schemas);

        if array_schemas.is_empty() {
            return Err(SchemaError::NoArrayFound);
        }

        // Determine primary recommendation
        let primary_recommendation = select_primary_schema(&array_schemas);

        Ok(SchemaDetection::new(
            array_schemas,
            primary_recommendation,
            None,
        ))
    }

    /// Analyze JSON and automatically create a GraphConfig.
    ///
    /// This is a convenience method that combines analysis and config generation.
    pub fn infer_graph_config(data: &Value) -> SchemaResult<GraphConfig> {
        let detection = Self::analyze(data)?;
        detection
            .to_graph_config()
            .ok_or_else(|| SchemaError::InvalidJson("Could not infer graph config".to_string()))
    }
}

/// Detect if a root object should be treated as a node.
///
/// A root object should be treated as a node when:
/// 1. It's an object (not an array)
/// 2. It contains one or more arrays of objects
/// 3. Those arrays have ID fields (they represent nodes)
///
/// This is typical for structures like:
/// ```json
/// {
///   "id": "patent-123",
///   "title": "My Patent",
///   "claims": [{"id": "c1", ...}, {"id": "c2", ...}]
/// }
/// ```
fn detect_root_object_node(root_obj: &serde_json::Map<String, Value>) -> Option<RootObjectSchema> {
    let mut fields = Vec::new();
    let mut related_arrays = Vec::new();

    // Check if root has an ID field
    let root_id_field = root_obj
        .keys()
        .find(|k| *k == "id" || k.contains("id"))
        .cloned();

    // If no ID field, this might not be a node-worthy object
    let id_field = root_id_field?;

    // Analyze each field in the root object
    for (key, value) in root_obj {
        match value {
            Value::Array(arr) => {
                // Check if this is an array of objects that could be related nodes
                if !arr.is_empty() {
                    if let Some(array_schema) = analyze_array_for_relation(key, arr) {
                        related_arrays.push(array_schema);
                    }
                }
            }
            Value::String(_) | Value::Number(_) | Value::Bool(_) => {
                // Primitive fields on the root object
                let field_type = match value {
                    Value::String(_) => FieldType::String,
                    Value::Number(_) => FieldType::Number,
                    Value::Bool(_) => FieldType::Boolean,
                    _ => unreachable!(),
                };
                fields.push(NodeFieldInfo {
                    name: key.clone(),
                    field_type,
                    is_id_candidate: key == "id" || key.contains("id"),
                    is_label_candidate: matches!(key.as_str(), "type" | "role" | "kind"),
                    is_relation_candidate: false,
                });
            }
            Value::Object(_) => {
                fields.push(NodeFieldInfo {
                    name: key.clone(),
                    field_type: FieldType::Object,
                    is_id_candidate: false,
                    is_label_candidate: false,
                    is_relation_candidate: false,
                });
            }
            Value::Null => {
                // Skip null fields
            }
        }
    }

    // Only treat as root object node if we have related node arrays
    if related_arrays.is_empty() {
        return None;
    }

    // Recommend label field if found
    let recommended_label_field = fields
        .iter()
        .find(|f| f.is_label_candidate)
        .map(|f| f.name.clone());

    Some(RootObjectSchema {
        path: String::new(),
        fields,
        recommended_id_field: Some(id_field),
        recommended_label_field,
        related_node_arrays: related_arrays,
    })
}

/// Analyze an array to determine if it represents related nodes.
///
/// Returns None if the array doesn't look like a collection of nodes.
fn analyze_array_for_relation(field_name: &str, arr: &[Value]) -> Option<RelatedNodeArraySchema> {
    // Must be non-empty
    if arr.is_empty() {
        return None;
    }

    // All elements should be objects
    if !arr.iter().all(|v| v.is_object()) {
        return None;
    }

    // Collect all fields across all elements
    let mut all_fields: HashMap<String, usize> = HashMap::new();
    for element in arr {
        if let Value::Object(obj) = element {
            for key in obj.keys() {
                *all_fields.entry(key.clone()).or_insert(0) += 1;
            }
        }
    }

    // Recommend ID field
    let recommended_id_field = all_fields
        .keys()
        .find(|k| *k == "id" || k.contains("id"))
        .cloned();

    // We need an ID field to create nodes
    let id_field = recommended_id_field?;

    // Recommend label field
    let recommended_label_field = all_fields
        .keys()
        .find(|k| matches!(k.as_str(), "type" | "role" | "kind" | "label"))
        .cloned();

    // Infer relationship type from field name
    let relationship_type = infer_relationship_type(field_name);

    Some(RelatedNodeArraySchema {
        field_name: field_name.to_string(),
        relationship_type,
        element_count: arr.len(),
        recommended_id_field: Some(id_field),
        recommended_label_field,
    })
}

/// Find all arrays in the JSON document.
fn find_arrays(data: &Value, current_path: &str, results: &mut Vec<ArraySchema>) {
    match data {
        Value::Array(arr) => {
            if !arr.is_empty() {
                if let Ok(schema) = analyze_array(arr, current_path) {
                    results.push(schema);
                }
            }
        }
        Value::Object(obj) => {
            for (key, value) in obj {
                let new_path = if current_path.is_empty() {
                    key.clone()
                } else {
                    format!("{}.{}", current_path, key)
                };
                find_arrays(value, &new_path, results);
            }
        }
        _ => {}
    }
}

/// Analyze an array to extract schema information.
fn analyze_array(arr: &[Value], path: &str) -> SchemaResult<ArraySchema> {
    let element_count = arr.len();
    let mut all_fields: HashMap<String, Vec<&Value>> = HashMap::new();
    let mut field_counts: HashMap<String, usize> = HashMap::new();

    // Collect all fields and their values
    for element in arr {
        if let Value::Object(obj) = element {
            for (key, value) in obj {
                all_fields.entry(key.clone()).or_default().push(value);
                *field_counts.entry(key.clone()).or_insert(0) += 1;
            }
        }
    }

    // Analyze each field
    let mut fields = Vec::new();
    for (field_name, values) in &all_fields {
        let occurrence_count = *field_counts.get(field_name).unwrap_or(&0);
        let field_info = analyze_field(field_name, values, occurrence_count, element_count);
        fields.push(field_info);
    }

    // Sort by occurrence count (most common first)
    fields.sort_by(|a, b| {
        let a_count = field_counts.get(&a.name).unwrap_or(&0);
        let b_count = field_counts.get(&b.name).unwrap_or(&0);
        b_count.cmp(a_count)
    });

    // Recommend ID field (prefer "id", "_id", or fields with unique string values)
    let recommended_id_field = fields
        .iter()
        .find(|f| f.is_id_candidate)
        .map(|f| f.name.clone())
        .or_else(|| {
            fields
                .iter()
                .find(|f| f.name == "id" || f.name == "_id")
                .map(|f| f.name.clone())
        });

    // Recommend label field (prefer "type", "role", "kind", "category")
    let recommended_label_field = fields
        .iter()
        .find(|f| f.is_label_candidate)
        .map(|f| f.name.clone())
        .or_else(|| {
            fields
                .iter()
                .find(|f| {
                    matches!(
                        f.name.as_str(),
                        "type" | "role" | "kind" | "category" | "label" | "class"
                    )
                })
                .map(|f| f.name.clone())
        });

    // Recommend relation fields (arrays that look like IDs)
    let recommended_relation_fields = fields
        .iter()
        .filter(|f| f.is_relation_candidate)
        .map(|f| f.name.clone())
        .collect();

    Ok(ArraySchema {
        path: path.to_string(),
        element_count,
        fields,
        recommended_id_field,
        recommended_label_field,
        recommended_relation_fields,
    })
}

/// Analyze a single field to determine its characteristics.
fn analyze_field(
    name: &str,
    values: &[&Value],
    occurrence_count: usize,
    total_elements: usize,
) -> NodeFieldInfo {
    let mut field_types = HashSet::new();
    let mut unique_strings = HashSet::new();
    let mut array_element_types = HashSet::new();

    for value in values {
        match value {
            Value::String(s) => {
                field_types.insert(FieldType::String);
                unique_strings.insert(s.clone());
            }
            Value::Number(_) => {
                field_types.insert(FieldType::Number);
            }
            Value::Bool(_) => {
                field_types.insert(FieldType::Boolean);
            }
            Value::Array(arr) => {
                field_types.insert(FieldType::Array);
                for elem in arr {
                    match elem {
                        Value::String(_) => array_element_types.insert(FieldType::String),
                        Value::Number(_) => array_element_types.insert(FieldType::Number),
                        _ => false,
                    };
                }
            }
            Value::Object(_) => {
                field_types.insert(FieldType::Object);
            }
            Value::Null => {
                field_types.insert(FieldType::Null);
            }
        }
    }

    // Determine field type (dominant type)
    let field_type = if field_types.len() == 1 {
        field_types.into_iter().next().unwrap_or(FieldType::Null)
    } else if field_types.contains(&FieldType::String) {
        FieldType::String
    } else {
        FieldType::Null
    };

    // Check if this could be an ID field
    let is_id_candidate = (name == "id" || name == "_id" || name.contains("id"))
        && occurrence_count == total_elements
        && unique_strings.len() == total_elements;

    // Check if this could be a label field
    let is_label_candidate = (name == "type" || name == "role" || name == "kind")
        && occurrence_count >= total_elements / 2
        && unique_strings.len() < total_elements;

    // Check if this could be a relation field (array of IDs)
    let is_relation_candidate = matches!(field_type, FieldType::Array)
        && array_element_types.len() <= 2
        && (array_element_types.contains(&FieldType::String)
            || array_element_types.contains(&FieldType::Number));

    NodeFieldInfo {
        name: name.to_string(),
        field_type,
        is_id_candidate,
        is_label_candidate,
        is_relation_candidate,
    }
}

/// Select the primary schema (the most suitable array for graph construction).
fn select_primary_schema(schemas: &[ArraySchema]) -> Option<ArraySchema> {
    if schemas.is_empty() {
        return None;
    }

    // Score each schema and select the best one
    let mut best_schema = None;
    let mut best_score = -1i32;

    for schema in schemas {
        let mut score = 0i32;

        // Prefer arrays with more elements (more data)
        score += (schema.element_count as i32).min(100);

        // Prefer arrays with an ID field
        if schema.recommended_id_field.is_some() {
            score += 200;
        }

        // Prefer arrays with a label field
        if schema.recommended_label_field.is_some() {
            score += 100;
        }

        // Prefer arrays with relation fields
        score += schema.recommended_relation_fields.len() as i32 * 50;

        // Prefer shorter paths (more direct access)
        let path_depth = schema.path.matches('.').count();
        score -= (path_depth as i32) * 10;

        // Prefer certain path names
        if schema.path.contains("node")
            || schema.path.contains("user")
            || schema.path.contains("item")
        {
            score += 50;
        }

        if score > best_score {
            best_score = score;
            best_schema = Some(schema.clone());
        }
    }

    best_schema
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_analyze_simple_users() {
        let data = json!({
            "users": [
                { "id": "1", "role": "admin", "age": 30 },
                { "id": "2", "role": "user", "age": 25 }
            ]
        });

        let result = SchemaAnalyzer::analyze(&data).unwrap();
        assert_eq!(result.array_schemas.len(), 1);
        assert!(result.primary_recommendation.is_some());

        let schema = &result.primary_recommendation.as_ref().unwrap();
        assert_eq!(schema.path, "users");
        assert_eq!(schema.recommended_id_field, Some("id".to_string()));
        assert_eq!(schema.recommended_label_field, Some("role".to_string()));
    }

    #[test]
    fn test_analyze_with_relations() {
        let data = json!({
            "users": [
                { "id": "1", "name": "Alice", "friends": ["2", "3"] },
                { "id": "2", "name": "Bob", "friends": ["1"] }
            ]
        });

        let result = SchemaAnalyzer::analyze(&data).unwrap();
        let schema = &result.primary_recommendation.as_ref().unwrap();
        assert_eq!(schema.recommended_id_field, Some("id".to_string()));
        assert!(schema
            .recommended_relation_fields
            .contains(&"friends".to_string()));
    }

    #[test]
    fn test_infer_graph_config() {
        let data = json!({
            "users": [
                { "id": "1", "role": "admin", "friends": ["2"] },
                { "id": "2", "role": "user", "friends": ["1"] }
            ]
        });

        let config = SchemaAnalyzer::infer_graph_config(&data).unwrap();
        assert_eq!(config.node_path, "users");
        assert_eq!(config.id_field, "id");
        assert_eq!(config.label_field, Some("role".to_string()));
        assert_eq!(config.relation_fields, vec!["friends"]);
    }

    #[test]
    fn test_nested_path() {
        let data = json!({
            "data": {
                "users": [
                    { "id": "1", "type": "Person" }
                ]
            }
        });

        let result = SchemaAnalyzer::analyze(&data).unwrap();
        let schema = &result.primary_recommendation.as_ref().unwrap();
        assert_eq!(schema.path, "data.users");
        assert_eq!(schema.recommended_id_field, Some("id".to_string()));
        assert_eq!(schema.recommended_label_field, Some("type".to_string()));
    }

    #[test]
    fn test_multiple_arrays() {
        let data = json!({
            "users": [{ "id": "1", "role": "admin" }],
            "posts": [{ "id": "p1", "author_id": "1" }]
        });

        let result = SchemaAnalyzer::analyze(&data).unwrap();
        assert_eq!(result.array_schemas.len(), 2);
        // Should prefer "users" as it has a clear ID and label
        let primary = result.primary_recommendation.as_ref().unwrap();
        assert_eq!(primary.path, "users");
    }

    #[test]
    fn test_empty_json() {
        let data = json!({});
        let result = SchemaAnalyzer::analyze(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_no_id_field() {
        let data = json!({
            "items": [
                { "name": "Alice" },
                { "name": "Bob" }
            ]
        });

        let result = SchemaAnalyzer::analyze(&data).unwrap();
        let schema = &result.primary_recommendation.as_ref().unwrap();
        // Should still work, but no ID field recommendation
        assert_eq!(schema.path, "items");
    }
}
