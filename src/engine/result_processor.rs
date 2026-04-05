use crate::parser::ast;
use serde_json::Value;

use super::QueryResult;

/// Remove duplicate rows from a query result.
pub fn deduplicate_rows(result: &mut QueryResult) {
    let mut seen = std::collections::HashSet::new();
    result.rows.retain(|row| {
        let serialized = serde_json::to_string(row).unwrap_or_default();
        seen.insert(serialized)
    });
}

/// Sort rows in a query result according to an ORDER BY clause.
pub fn sort_rows(result: &mut QueryResult, order_by: &ast::OrderByClause) {
    result.rows.sort_by(|a, b| {
        for item in &order_by.items {
            let col_key = if let Some(ref prop) = item.expression.property {
                format!("{}.{}", item.expression.variable, prop)
            } else {
                item.expression.variable.clone()
            };
            let a_val = a.get(&col_key);
            let b_val = b.get(&col_key);
            let ord = compare_values(a_val, b_val);
            let cmp = if item.direction == ast::SortDirection::Desc {
                ord.reverse()
            } else {
                ord
            };
            if cmp != std::cmp::Ordering::Equal {
                return cmp;
            }
        }
        std::cmp::Ordering::Equal
    });
}

fn compare_values(a: Option<&Value>, b: Option<&Value>) -> std::cmp::Ordering {
    match (a, b) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(va), Some(vb)) => match (va.as_i64(), vb.as_i64()) {
            (Some(na), Some(nb)) => na.cmp(&nb),
            _ => {
                let sa = va.as_str().unwrap_or_default();
                let sb = vb.as_str().unwrap_or_default();
                // Try numeric comparison for string-represented numbers
                if let (Ok(na), Ok(nb)) = (sa.parse::<f64>(), sb.parse::<f64>()) {
                    na.partial_cmp(&nb).unwrap_or(std::cmp::Ordering::Equal)
                } else {
                    sa.cmp(sb)
                }
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::executor::QueryExecutor;
    use crate::graph::Graph;
    use crate::graph::Node;
    use crate::parser;
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
    fn test_deduplicate_rows() {
        let mut graph = Graph::new();
        graph.add_node(Node::new("1".to_string(), None, json!({"name": "Alice"})));
        graph.add_node(Node::new("2".to_string(), None, json!({"name": "Alice"})));
        graph.add_node(Node::new("3".to_string(), None, json!({"name": "Bob"})));

        let parsed = parser::parse_query("MATCH (n) RETURN DISTINCT n.name").unwrap();
        let result = QueryExecutor::execute(&parsed, &graph).unwrap();
        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn test_sort_asc() {
        let graph = create_test_graph();
        let parsed = parser::parse_query("MATCH (n) RETURN n.age ORDER BY n.age ASC").unwrap();
        let result = QueryExecutor::execute(&parsed, &graph).unwrap();
        assert_eq!(result.rows.len(), 3);
        let ages: Vec<i64> = result
            .rows
            .iter()
            .map(|r| r.get("n.age").unwrap().as_i64().unwrap())
            .collect();
        assert_eq!(ages, vec![25, 30, 35]);
    }

    #[test]
    fn test_sort_desc() {
        let graph = create_test_graph();
        let parsed = parser::parse_query("MATCH (n) RETURN n.age ORDER BY n.age DESC").unwrap();
        let result = QueryExecutor::execute(&parsed, &graph).unwrap();
        let ages: Vec<i64> = result
            .rows
            .iter()
            .map(|r| r.get("n.age").unwrap().as_i64().unwrap())
            .collect();
        assert_eq!(ages, vec![35, 30, 25]);
    }

    #[test]
    fn test_sort_string() {
        let mut graph = Graph::new();
        graph.add_node(Node::new(
            "1".to_string(),
            Some("user".to_string()),
            json!({"name": "Charlie"}),
        ));
        graph.add_node(Node::new(
            "2".to_string(),
            Some("user".to_string()),
            json!({"name": "Alice"}),
        ));
        graph.add_node(Node::new(
            "3".to_string(),
            Some("user".to_string()),
            json!({"name": "Bob"}),
        ));

        let parsed = parser::parse_query("MATCH (n) RETURN n.name ORDER BY n.name DESC").unwrap();
        let result = QueryExecutor::execute(&parsed, &graph).unwrap();
        let names: Vec<&str> = result
            .rows
            .iter()
            .map(|r| r.get("n.name").unwrap().as_str().unwrap())
            .collect();
        assert_eq!(names, vec!["Charlie", "Bob", "Alice"]);
    }

    #[test]
    fn test_sort_default_asc() {
        let graph = create_test_graph();
        let parsed = parser::parse_query("MATCH (n) RETURN n.age ORDER BY n.age").unwrap();
        let result = QueryExecutor::execute(&parsed, &graph).unwrap();
        let ages: Vec<i64> = result
            .rows
            .iter()
            .map(|r| r.get("n.age").unwrap().as_i64().unwrap())
            .collect();
        assert_eq!(ages, vec![25, 30, 35]);
    }

    #[test]
    fn test_sort_multiple_columns() {
        let mut graph = Graph::new();
        graph.add_node(Node::new(
            "1".to_string(),
            None,
            json!({"role": "admin", "age": 35}),
        ));
        graph.add_node(Node::new(
            "2".to_string(),
            None,
            json!({"role": "user", "age": 25}),
        ));
        graph.add_node(Node::new(
            "3".to_string(),
            None,
            json!({"role": "admin", "age": 30}),
        ));

        let parsed =
            parser::parse_query("MATCH (n) RETURN n.role, n.age ORDER BY n.role ASC, n.age DESC")
                .unwrap();
        let result = QueryExecutor::execute(&parsed, &graph).unwrap();
        let rows: Vec<(String, i64)> = result
            .rows
            .iter()
            .map(|r| {
                (
                    r.get("n.role").unwrap().as_str().unwrap().to_string(),
                    r.get("n.age").unwrap().as_i64().unwrap(),
                )
            })
            .collect();
        assert_eq!(
            rows,
            vec![
                ("admin".to_string(), 35),
                ("admin".to_string(), 30),
                ("user".to_string(), 25)
            ]
        );
    }

    #[test]
    fn test_sort_with_distinct() {
        let graph = create_test_graph();
        let parsed =
            parser::parse_query("MATCH (n) RETURN DISTINCT n.role ORDER BY n.role").unwrap();
        let result = QueryExecutor::execute(&parsed, &graph).unwrap();
        assert_eq!(result.rows.len(), 2);
        let roles: Vec<&str> = result
            .rows
            .iter()
            .map(|r| r.get("n.role").unwrap().as_str().unwrap())
            .collect();
        assert_eq!(roles, vec!["admin", "user"]);
    }
}
