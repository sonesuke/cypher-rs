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

    /// Get the best GraphConfig based on detected schema.
    pub fn to_graph_config(&self) -> Option<GraphConfig> {
        let schema = &self.primary_recommendation.as_ref()?;
        let id_field = schema.recommended_id_field.clone()?;
        let label_field = schema.recommended_label_field.clone();

        Some(GraphConfig {
            node_path: schema.path.clone(),
            id_field,
            label_field,
            relation_fields: schema.recommended_relation_fields.clone(),
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
        detection
            .to_graph_config()
            .ok_or_else(|| SchemaError::InvalidJson("Could not infer graph config".to_string()))
    }
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
