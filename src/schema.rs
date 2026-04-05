use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fmt;

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

/// Schema for a root object that contains nested arrays.
#[derive(Debug, Clone)]
pub struct RootObjectSchema {
    /// The recommended label for the root node
    pub label: String,
    /// Nested arrays found in the root object
    pub nested_arrays: Vec<ArraySchema>,
}

/// Schema detection result for a JSON document.
#[derive(Debug, Clone)]
pub struct SchemaDetection {
    /// All detected array schemas
    pub array_schemas: Vec<ArraySchema>,
    /// Root object schema
    pub root_object: Option<RootObjectSchema>,
}

impl SchemaDetection {
    /// Create a schema detection result with root object info.
    fn with_root_object(
        array_schemas: Vec<ArraySchema>,
        root_object: RootObjectSchema,
    ) -> Self {
        Self {
            array_schemas,
            root_object: Some(root_object),
        }
    }

    /// Whether this schema represents a root object.
    pub fn is_root_object(&self) -> bool {
        self.root_object.is_some()
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
    /// assert!(schema.is_root_object());
    /// ```
    pub fn analyze(data: &Value) -> SchemaResult<SchemaDetection> {
        let obj = data
            .as_object()
            .ok_or(SchemaError::NoArrayFound)?;

        let root_schema = detect_root_object(obj);
        if root_schema.nested_arrays.is_empty() {
            return Err(SchemaError::NoArrayFound);
        }

        Ok(SchemaDetection::with_root_object(
            root_schema.nested_arrays.clone(),
            root_schema,
        ))
    }
}

/// Detect root object schema.
fn detect_root_object(obj: &serde_json::Map<String, Value>) -> RootObjectSchema {
    let mut nested_arrays = Vec::new();

    for (key, value) in obj {
        let elements: Vec<&Value> = match value {
            Value::Array(arr) => {
                if arr.is_empty() || !arr.first().map_or(false, |v| v.is_object()) {
                    continue;
                }
                arr.iter().collect()
            }
            Value::Object(_) => vec![value],
            _ => continue,
        };

        let mut all_fields: HashMap<String, usize> = HashMap::new();
        let mut field_values: HashMap<String, HashSet<Value>> = HashMap::new();
        let element_count = elements.len();

        for element in &elements {
            if let Value::Object(elem_obj) = element {
                for (fkey, fvalue) in elem_obj {
                    *all_fields.entry(fkey.clone()).or_insert(0) += 1;
                    field_values
                        .entry(fkey.clone())
                        .or_default()
                        .insert(fvalue.clone());
                }
            }
        }

        let mut fields = Vec::new();
        for field_name in all_fields.keys() {
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

            let is_id_candidate = field_name.contains("id")
                || field_name == "key"
                || field_name == "uuid"
                || field_name == "_id";

            let is_relation_candidate =
                field_type == FieldType::Array && !is_id_candidate;

            fields.push(NodeFieldInfo {
                name: field_name.clone(),
                field_type,
                is_id_candidate,
                is_relation_candidate,
            });
        }

        let recommended_id_field = fields
            .iter()
            .find(|f| f.is_id_candidate)
            .map(|f| f.name.clone());

        let recommended_relation_fields: Vec<String> = fields
            .iter()
            .filter(|f| f.is_relation_candidate)
            .map(|f| f.name.clone())
            .collect();

        nested_arrays.push(ArraySchema {
            path: key.clone(),
            element_count,
            fields,
            field_values,
            recommended_id_field,
            recommended_relation_fields,
        });
    }

    RootObjectSchema {
        label: "Root".to_string(),
        nested_arrays,
    }
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
    fn test_root_object_detection() {
        let data = json!({
            "id": "US123",
            "title": "Test Patent",
            "claims": [
                { "id": "c1", "number": "1", "text": "Claim 1" },
                { "id": "c2", "number": "2", "text": "Claim 2" }
            ]
        });

        let schema = SchemaAnalyzer::analyze(&data).unwrap();
        assert!(schema.is_root_object());

        let root = schema.root_object.unwrap();
        assert_eq!(root.label, "Root");
        assert_eq!(root.nested_arrays.len(), 1);
        assert_eq!(root.nested_arrays[0].path, "claims");
        assert_eq!(root.nested_arrays[0].element_count, 2);
    }

    #[test]
    fn test_root_object_multiple_arrays() {
        let data = json!({
            "id": "doc-1",
            "sections": [
                { "id": "s1", "heading": "Intro" }
            ],
            "authors": [
                { "id": "a1", "name": "Alice" }
            ]
        });

        let schema = SchemaAnalyzer::analyze(&data).unwrap();
        assert!(schema.is_root_object());

        let root = schema.root_object.unwrap();
        assert_eq!(root.nested_arrays.len(), 2);
        let paths: Vec<&str> = root.nested_arrays.iter().map(|a| a.path.as_str()).collect();
        assert!(paths.contains(&"sections"));
        assert!(paths.contains(&"authors"));
    }

    #[test]
    fn test_object_values() {
        let data = json!({
            "object1": { "prop1": "x", "prop2": "y" },
            "object2": { "prop3": "z" }
        });

        let schema = SchemaAnalyzer::analyze(&data).unwrap();
        assert!(schema.is_root_object());

        let root = schema.root_object.unwrap();
        assert_eq!(root.nested_arrays.len(), 2);
        let paths: Vec<&str> = root.nested_arrays.iter().map(|a| a.path.as_str()).collect();
        assert!(paths.contains(&"object1"));
        assert!(paths.contains(&"object2"));
    }
}
