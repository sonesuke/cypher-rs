use crate::engine::functions::EvalContext;
use crate::graph::Graph;
use crate::parser::ast;
use serde_json::Value;
use std::collections::HashMap;

use super::{EngineError, QueryResult, Result};

/// Entity ID type for tracking matched nodes and relationships during query execution.
#[derive(Debug, Clone, PartialEq)]
pub enum EntityId {
    Node(usize),
    Relationship {
        from_idx: usize,
        to_idx: usize,
        rel: String,
    },
}

/// Type alias for variable bindings during query execution.
pub type Bindings = HashMap<String, EntityId>;

/// Cypher query executor.
///
/// Executes parsed Cypher queries against a graph.
pub struct QueryExecutor;

impl QueryExecutor {
    /// Execute a parsed query against a graph.
    pub fn execute(query: &ast::Query, graph: &Graph) -> Result<QueryResult> {
        // 1. Match patterns
        let mut bindings_list: Vec<Bindings> = vec![HashMap::new()];

        for pattern_part in &query.match_clause.patterns {
            let mut last_node_variable: Option<String> = None;

            for chain in &pattern_part.chains {
                match chain {
                    ast::PatternChain::Node(node_pat) => {
                        if let Some(ref v) = node_pat.variable {
                            last_node_variable = Some(v.clone());
                        }
                        bindings_list = Self::match_node_pattern(node_pat, graph, bindings_list);
                    }
                    ast::PatternChain::Relationship(rel_pat, node_pat) => {
                        if let Some(ref start_var) = last_node_variable {
                            bindings_list = Self::match_relationship_pattern(
                                start_var,
                                rel_pat,
                                node_pat,
                                graph,
                                bindings_list,
                            );

                            if let Some(ref v) = node_pat.variable {
                                last_node_variable = Some(v.clone());
                            }
                        }
                    }
                }
            }
        }

        // 2. Filter with WHERE
        if let Some(where_clause) = &query.where_clause {
            bindings_list.retain(|bindings| {
                Self::evaluate_expression(&where_clause.expression, bindings, graph)
            });
        }

        // 3. Project with RETURN
        let has_aggregate = query
            .return_clause
            .items
            .iter()
            .any(|item| matches!(&item.expression, ast::Expression::Aggregate(_)));

        if has_aggregate {
            Self::execute_aggregate_return(&query.return_clause, bindings_list, graph)
        } else {
            Self::execute_normal_return(&query.return_clause, bindings_list, graph)
        }
    }

    fn execute_aggregate_return(
        return_clause: &ast::ReturnClause,
        bindings_list: Vec<Bindings>,
        graph: &Graph,
    ) -> Result<QueryResult> {
        use crate::engine::functions::AggregateEvaluator;

        let mut columns = Vec::new();
        let mut values = serde_json::Map::new();

        for item in &return_clause.items {
            let column_name = item.alias.clone().unwrap_or_else(|| {
                if let ast::Expression::Aggregate(agg) = &item.expression {
                    AggregateEvaluator::column_name(agg)
                } else {
                    Self::expression_column_name(&item.expression)
                }
            });

            let value = match &item.expression {
                ast::Expression::Aggregate(agg) => {
                    // Convert bindings to EvalContexts
                    let contexts: Vec<EvalContext> = bindings_list
                        .iter()
                        .map(|bindings| {
                            let mut ctx = EvalContext::new();
                            for (var, entity) in bindings {
                                if let EntityId::Node(idx) = entity {
                                    ctx.bind(var.clone(), *idx);
                                }
                            }
                            ctx
                        })
                        .collect();

                    AggregateEvaluator::evaluate(agg, &contexts, graph)
                        .map_err(|e| EngineError::ExecutionError(e.to_string()))?
                }
                _ => {
                    return Err(EngineError::ExecutionError(
                        "Mixed aggregate and non-aggregate in RETURN".to_string(),
                    ))
                }
            };

            columns.push(column_name.clone());
            values.insert(column_name, value);
        }

        Ok(QueryResult {
            columns,
            rows: vec![Value::Object(values)],
        })
    }

    fn execute_normal_return(
        return_clause: &ast::ReturnClause,
        bindings_list: Vec<Bindings>,
        graph: &Graph,
    ) -> Result<QueryResult> {
        let mut columns = Vec::new();
        let mut rows = Vec::new();

        for item in &return_clause.items {
            let column_name = item
                .alias
                .clone()
                .unwrap_or_else(|| Self::expression_column_name(&item.expression));
            columns.push(column_name);
        }

        for bindings in bindings_list {
            let mut row = serde_json::Map::new();

            for (i, item) in return_clause.items.iter().enumerate() {
                let column_name = &columns[i];
                let value = Self::evaluate_expression_value(&item.expression, &bindings, graph);
                row.insert(column_name.clone(), value);
            }

            rows.push(Value::Object(row));
        }

        Ok(QueryResult { columns, rows })
    }

    fn expression_column_name(expr: &ast::Expression) -> String {
        match expr {
            ast::Expression::Comparison(comp)
                if comp.operator.is_none() && comp.right.is_none() =>
            {
                if let Some(ref prop) = comp.left.property {
                    format!("{}.{}", comp.left.variable, prop)
                } else {
                    comp.left.variable.clone()
                }
            }
            ast::Expression::Aggregate(agg) => {
                let func_name = match agg.func {
                    ast::AggregateFunction::Count => "COUNT",
                    ast::AggregateFunction::Sum => "SUM",
                };

                if let Some(ref prop) = agg.property {
                    format!("{}({}.{})", func_name, agg.variable, prop)
                } else {
                    format!("{}({})", func_name, agg.variable)
                }
            }
            _ => "expression".to_string(),
        }
    }

    fn match_node_pattern(
        node_pat: &ast::NodePattern,
        graph: &Graph,
        current_bindings: Vec<Bindings>,
    ) -> Vec<Bindings> {
        let mut next_bindings = Vec::new();

        for bindings in current_bindings {
            for (i, node) in graph.nodes.iter().enumerate() {
                // Check labels
                let label_match = if node_pat.labels.is_empty() {
                    true
                } else {
                    node_pat
                        .labels
                        .iter()
                        .any(|l| node.label.as_ref() == Some(l))
                };

                if !label_match {
                    continue;
                }

                // Bind variable
                if let Some(ref var) = node_pat.variable {
                    if let Some(entity) = bindings.get(var) {
                        if let EntityId::Node(prev_idx) = entity {
                            if *prev_idx == i {
                                next_bindings.push(bindings.clone());
                            }
                        }
                    } else {
                        let mut new_bindings = bindings.clone();
                        new_bindings.insert(var.clone(), EntityId::Node(i));
                        next_bindings.push(new_bindings);
                    }
                } else {
                    next_bindings.push(bindings.clone());
                }
            }
        }
        next_bindings
    }

    fn match_relationship_pattern(
        start_node_var: &str,
        rel_pat: &ast::RelationshipPattern,
        end_node_pat: &ast::NodePattern,
        graph: &Graph,
        current_bindings: Vec<Bindings>,
    ) -> Vec<Bindings> {
        let mut next_bindings = Vec::new();

        // Build adjacency maps
        let mut forward_adj: HashMap<usize, Vec<(usize, String)>> = HashMap::new();
        let mut backward_adj: HashMap<usize, Vec<(usize, String)>> = HashMap::new();

        for edge in &graph.edges {
            forward_adj
                .entry(edge.from)
                .or_default()
                .push((edge.to, edge.rel_type.clone()));
            backward_adj
                .entry(edge.to)
                .or_default()
                .push((edge.from, edge.rel_type.clone()));
        }

        for bindings in current_bindings {
            if let Some(EntityId::Node(start_idx)) = bindings.get(start_node_var) {
                let start_idx = *start_idx;

                // Single hop matching
                let neighbors = match rel_pat.direction {
                    ast::Direction::Right => {
                        forward_adj.get(&start_idx).cloned().unwrap_or_default()
                    }
                    ast::Direction::Left => {
                        backward_adj.get(&start_idx).cloned().unwrap_or_default()
                    }
                    ast::Direction::Both => {
                        let mut neighbors =
                            forward_adj.get(&start_idx).cloned().unwrap_or_default();
                        neighbors.extend(backward_adj.get(&start_idx).cloned().unwrap_or_default());
                        neighbors
                    }
                };

                for (next_idx, rel) in neighbors {
                    // Check rel_type if specified
                    let rel_match = if let Some(ref target_rel_type) = rel_pat.rel_type {
                        &rel == target_rel_type
                    } else {
                        true
                    };

                    if !rel_match {
                        continue;
                    }

                    // Check if current node matches end_node_pat
                    let node = &graph.nodes[next_idx];
                    let label_match = if end_node_pat.labels.is_empty() {
                        true
                    } else {
                        end_node_pat
                            .labels
                            .iter()
                            .any(|l| node.label.as_ref() == Some(l))
                    };

                    if label_match {
                        let mut new_bindings = bindings.clone();

                        // Bind relationship variable if present
                        if let Some(ref r_var) = rel_pat.variable {
                            new_bindings.insert(
                                r_var.clone(),
                                EntityId::Relationship {
                                    from_idx: start_idx,
                                    to_idx: next_idx,
                                    rel: rel.clone(),
                                },
                            );
                        }

                        // Bind end variable
                        if let Some(ref var) = end_node_pat.variable {
                            if let Some(EntityId::Node(prev_idx)) = bindings.get(var) {
                                if *prev_idx == next_idx {
                                    next_bindings.push(new_bindings);
                                }
                            } else {
                                new_bindings.insert(var.clone(), EntityId::Node(next_idx));
                                next_bindings.push(new_bindings);
                            }
                        } else {
                            next_bindings.push(new_bindings);
                        }
                    }
                }
            }
        }

        next_bindings
    }

    fn evaluate_expression(expr: &ast::Expression, bindings: &Bindings, graph: &Graph) -> bool {
        match expr {
            ast::Expression::And(exprs) => exprs
                .iter()
                .all(|e| Self::evaluate_expression(e, bindings, graph)),
            ast::Expression::Or(exprs) => exprs
                .iter()
                .any(|e| Self::evaluate_expression(e, bindings, graph)),
            ast::Expression::Comparison(comp) => {
                let left_val = Self::evaluate_property_or_variable(&comp.left, bindings, graph);

                if let Some(right_term) = &comp.right {
                    let right_val = match right_term {
                        ast::Term::Literal(lit) => match lit {
                            ast::Literal::String(s) => s.clone(),
                            ast::Literal::Number(n) => n.to_string(),
                        },
                        ast::Term::PropertyOrVariable(pv) => {
                            Self::evaluate_property_or_variable(pv, bindings, graph)
                        }
                    };

                    if let Some(op) = &comp.operator {
                        match op {
                            ast::ComparisonOperator::Eq => left_val == right_val,
                            ast::ComparisonOperator::NotEq => left_val != right_val,
                            ast::ComparisonOperator::Contains => left_val.contains(&right_val),
                            ast::ComparisonOperator::Lt => left_val < right_val,
                            ast::ComparisonOperator::Gt => left_val > right_val,
                            ast::ComparisonOperator::LtEq => left_val <= right_val,
                            ast::ComparisonOperator::GtEq => left_val >= right_val,
                        }
                    } else {
                        !left_val.is_empty() && left_val != "null"
                    }
                } else {
                    !left_val.is_empty() && left_val != "null"
                }
            }
            ast::Expression::Aggregate(_) => true,
        }
    }

    fn evaluate_expression_value(
        expr: &ast::Expression,
        bindings: &Bindings,
        graph: &Graph,
    ) -> Value {
        match expr {
            ast::Expression::Comparison(comp) => {
                if comp.operator.is_none() && comp.right.is_none() {
                    let val = Self::evaluate_property_or_variable(&comp.left, bindings, graph);
                    // Try to parse as number first
                    if let Ok(n) = val.parse::<i64>() {
                        Value::Number(n.into())
                    } else {
                        Value::String(val)
                    }
                } else {
                    Value::Bool(Self::evaluate_expression(expr, bindings, graph))
                }
            }
            ast::Expression::Aggregate(_) => Value::Null,
            _ => Value::Null,
        }
    }

    fn evaluate_property_or_variable(
        pv: &ast::PropertyOrVariable,
        bindings: &Bindings,
        graph: &Graph,
    ) -> String {
        if let Some(entity) = bindings.get(&pv.variable) {
            match entity {
                EntityId::Node(idx) => {
                    let node = &graph.nodes[*idx];
                    if let Some(ref prop) = pv.property {
                        node.get_property_as_string(prop)
                            .unwrap_or_else(|| "null".to_string())
                    } else {
                        node.id.clone()
                    }
                }
                EntityId::Relationship { rel, .. } => {
                    if let Some(ref prop) = pv.property {
                        if prop == "type" {
                            rel.clone()
                        } else {
                            "null".to_string()
                        }
                    } else {
                        rel.clone()
                    }
                }
            }
        } else {
            "null".to_string()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
    fn test_execute_match_all() {
        let graph = create_test_graph();
        let parsed = parser::parse_query("MATCH (n) RETURN n.id").unwrap();
        let result = QueryExecutor::execute(&parsed, &graph).unwrap();
        assert_eq!(result.rows.len(), 3);
    }

    #[test]
    fn test_execute_match_with_label() {
        let graph = create_test_graph();
        let parsed = parser::parse_query("MATCH (n:admin) RETURN n.id").unwrap();
        let result = QueryExecutor::execute(&parsed, &graph).unwrap();
        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn test_execute_count() {
        let graph = create_test_graph();
        let parsed = parser::parse_query("MATCH (n) RETURN COUNT(n)").unwrap();
        let result = QueryExecutor::execute(&parsed, &graph).unwrap();
        assert_eq!(result.columns[0], "COUNT(n)");
        assert_eq!(result.get_single_value().unwrap().as_i64(), Some(3));
    }

    #[test]
    fn test_execute_sum() {
        let graph = create_test_graph();
        let parsed = parser::parse_query("MATCH (n) RETURN SUM(n.age)").unwrap();
        let result = QueryExecutor::execute(&parsed, &graph).unwrap();
        assert_eq!(result.get_single_value().unwrap().as_i64(), Some(90));
    }
}
