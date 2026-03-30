use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fmt;

use crate::config::GraphConfig;

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

/// Detected schema for a JSON array.
#[derive(Debug, Clone)]
pub struct ArraySchema {
    /// Path to this array (e.g., "users" or "data.users")
    pub path: String,
    /// Number of elements in the array
    pub element_count: usize,
    /// Detected fields in the array elements
    pub fields: Vec<NodeFieldInfo>,
    /// All unique values found for each field
    pub field_values: HashMap<String, HashSet<Value>>,
    /// Recommended ID field for this array
    pub recommended_id_field: Option<String>,
    /// Fields that likely contain relationships (arrays of IDs)
    pub recommended_relation_fields: Vec<String>,
}

/// Schema detection result for a JSON document.
#[derive(Debug, Clone)]
pub struct SchemaDetection {
    /// All detected array schemas
    pub array_schemas: Vec<ArraySchema>,
    /// Primary recommendation (the most suitable array for graph construction)
    pub primary_recommendation: Option<ArraySchema>,
}

impl SchemaDetection {
    /// Create a new schema detection result.
    pub fn new(
        array_schemas: Vec<ArraySchema>,
        primary_recommendation: Option<ArraySchema>,
    ) -> Self {
        Self {
            array_schemas,
            primary_recommendation,
        }
    }

    /// Convert to a GraphConfig using the primary recommendation.
    pub fn to_graph_config(&self) -> Option<GraphConfig> {
        let schema = &self.primary_recommendation.as_ref()?;
        let id_field = schema.recommended_id_field.clone()?;

        Some(GraphConfig {
            node_path: schema.path.clone(),
            id_field,
            relation_fields: schema.recommended_relation_fields.clone(),
        })
    }

    /// Generate a Neo4j-style schema representation.
    pub fn to_neo4j_schema(&self) -> String {
        let mut output = String::from("Graph Schema\n============\n\n");

        if self.array_schemas.is_empty() {
            output.push_str("No node types detected.\n");
            return output;
        }

        output.push_str("Node Types:\n");
        for schema in &self.array_schemas {
            let label = schema.path.rsplit('.').next().unwrap_or(&schema.path);
            output.push_str(&format!("  (:{} {} nodes)\n", label, schema.element_count));
        }

        output.push_str("\nProperties:\n");
        for schema in &self.array_schemas {
            let label = schema.path.rsplit('.').next().unwrap_or(&schema.path);
            output.push_str(&format!(":{} {{", label));

            let mut field_strings: Vec<String> = schema
                .fields
                .iter()
                .map(|f| format!("{}: {}", f.name, f.field_type))
                .collect();

            field_strings.sort();
            output.push_str(&field_strings.join(", "));
            output.push_str("}\n");
        }

        // Relationship types
        let has_relations = self
            .array_schemas
            .iter()
            .any(|s| !s.recommended_relation_fields.is_empty());

        if has_relations {
            output.push_str("\nRelationship Types:\n");
            for schema in &self.array_schemas {
                let label = schema.path.rsplit('.').next().unwrap_or(&schema.path);
                for rel_field in &schema.recommended_relation_fields {
                    output.push_str(&format!("(:{})-[:{}]->()\n", label, rel_field));
                }
            }
        }

        output
    }

    /// Generate a compact pattern representation.
    pub fn to_pattern(&self) -> String {
        let mut patterns = Vec::new();

        for schema in &self.array_schemas {
            let label = schema.path.rsplit('.').next().unwrap_or(&schema.path);
            let pattern = if !schema.recommended_relation_fields.is_empty() {
                format!(
                    "(:{})-[{}]->(:{})",
                    label,
                    schema.recommended_relation_fields.join("|"),
                    label
                )
            } else {
                format!("(:{})", label)
            };

            patterns.push(pattern);
        }

        patterns.join(", ")
    }
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
        let mut array_schemas = Vec::new();
        find_arrays(data, "", &mut array_schemas);

        if array_schemas.is_empty() {
            return Err(SchemaError::NoArrayFound);
        }

        // Determine primary recommendation
        let primary_recommendation = select_primary_schema(&array_schemas);

        Ok(SchemaDetection::new(array_schemas, primary_recommendation))
    }

    /// Analyze JSON and automatically create a GraphConfig.
    ///
    /// This is a convenience method that combines analysis and config generation.
    pub fn infer_graph_config(data: &Value) -> SchemaResult<GraphConfig> {
        let detection = Self::analyze(data)?;
        detection.to_graph_config().ok_or(SchemaError::NoArrayFound)
    }
}

/// Find all arrays in the JSON document.
fn find_arrays(data: &Value, current_path: &str, results: &mut Vec<ArraySchema>) {
    match data {
        Value::Array(arr) => {
            if arr.is_empty() {
                return;
            }

            // Analyze array elements
            let mut all_fields: HashMap<String, usize> = HashMap::new();
            let mut field_values: HashMap<String, HashSet<Value>> = HashMap::new();
            let element_count = arr.len();

            for element in arr {
                if let Value::Object(obj) = element {
                    for (key, value) in obj {
                        *all_fields.entry(key.clone()).or_insert(0) += 1;
                        field_values
                            .entry(key.clone())
                            .or_default()
                            .insert(value.clone());
                    }
                }
            }

            // Detect field types
            let mut fields = Vec::new();
            for field_name in all_fields.keys() {
                // Determine field type based on values
                let values = field_values.get(field_name);
                let field_type = if let Some(vals) = values {
                    if vals.iter().all(|v| v.is_string()) {
                        FieldType::String
                    } else if vals.iter().all(|v| v.is_i64() || v.is_u64() || v.is_f64()) {
                        FieldType::Number
                    } else if vals.iter().all(|v| v.is_boolean()) {
                        FieldType::Boolean
                    } else if vals.iter().all(|v| v.is_array()) {
                        FieldType::Array
                    } else if vals.iter().all(|v| v.is_object()) {
                        FieldType::Object
                    } else {
                        FieldType::Null
                    }
                } else {
                    FieldType::Null
                };

                // Check if this could be an ID field
                let is_id_candidate = field_name.contains("id")
                    || field_name == "key"
                    || field_name == "uuid"
                    || field_name == "_id";

                // Check if this could be a relation field (array of IDs)
                let is_relation_candidate = field_type == FieldType::Array && !is_id_candidate;

                fields.push(NodeFieldInfo {
                    name: field_name.clone(),
                    field_type,
                    is_id_candidate,
                    is_relation_candidate,
                });
            }

            // Find recommended ID field
            let recommended_id_field = fields
                .iter()
                .find(|f| f.is_id_candidate)
                .map(|f| f.name.clone());

            // Find recommended relation fields
            let recommended_relation_fields: Vec<String> = fields
                .iter()
                .filter(|f| f.is_relation_candidate)
                .map(|f| f.name.clone())
                .collect();

            results.push(ArraySchema {
                path: current_path.to_string(),
                element_count,
                fields,
                field_values,
                recommended_id_field,
                recommended_relation_fields,
            });
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

/// Select the primary schema from detected schemas.
fn select_primary_schema(schemas: &[ArraySchema]) -> Option<ArraySchema> {
    if schemas.is_empty() {
        return None;
    }

    // Prefer arrays that have an ID field
    let with_id: Vec<_> = schemas
        .iter()
        .filter(|s| s.recommended_id_field.is_some())
        .collect();

    if with_id.is_empty() {
        return Some(schemas[0].clone());
    }

    // Among those with ID, prefer the shortest path (highest level)
    let mut best = &with_id[0];
    for schema in &with_id[1..] {
        if schema.path.len() < best.path.len() {
            best = schema;
        }
    }

    Some((*best).clone())
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

        let schema = SchemaAnalyzer::analyze(&data).unwrap();
        assert_eq!(schema.array_schemas.len(), 1);

        let users_schema = &schema.array_schemas[0];
        assert_eq!(users_schema.path, "users");
        assert_eq!(users_schema.element_count, 2);
        assert_eq!(users_schema.recommended_id_field, Some("id".to_string()));
    }

    #[test]
    fn test_analyze_with_relations() {
        let data = json!({
            "users": [
                { "id": "1", "name": "Alice", "friends": ["2", "3"] },
                { "id": "2", "name": "Bob", "friends": ["1"] }
            ]
        });

        let schema = SchemaAnalyzer::analyze(&data).unwrap();
        let users_schema = &schema.array_schemas[0];
        assert_eq!(
            users_schema.recommended_relation_fields,
            vec!["friends".to_string()]
        );
    }

    #[test]
    fn test_empty_json() {
        let data = json!({});
        let result = SchemaAnalyzer::analyze(&data);
        assert!(matches!(result, Err(SchemaError::NoArrayFound)));
    }

    #[test]
    fn test_no_id_field() {
        let data = json!({
            "users": [
                { "name": "Alice" },
                { "name": "Bob" }
            ]
        });

        let schema = SchemaAnalyzer::analyze(&data).unwrap();
        let users_schema = &schema.array_schemas[0];
        assert!(users_schema.recommended_id_field.is_none());
    }

    #[test]
    fn test_multiple_arrays() {
        let data = json!({
            "users": [{ "id": "1", "name": "Alice" }],
            "posts": [{ "id": "p1", "title": "Hello" }]
        });

        let schema = SchemaAnalyzer::analyze(&data).unwrap();
        assert_eq!(schema.array_schemas.len(), 2);
    }

    #[test]
    fn test_infer_graph_config() {
        let data = json!({
            "users": [
                { "id": "1", "role": "admin", "age": 30 }
            ]
        });

        let schema = SchemaAnalyzer::analyze(&data).unwrap();
        let config = schema.to_graph_config().unwrap();
        assert_eq!(config.node_path, "users");
        assert_eq!(config.id_field, "id");
    }

    #[test]
    fn test_nested_path() {
        let data = json!({
            "data": {
                "users": [
                    { "id": "1", "name": "Alice" }
                ]
            }
        });

        let schema = SchemaAnalyzer::analyze(&data).unwrap();
        assert_eq!(schema.array_schemas[0].path, "data.users");
    }
}
