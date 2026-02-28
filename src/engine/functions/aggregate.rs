use crate::graph::Graph;
use crate::parser::ast;
use serde_json::Value;

use super::{EvalContext, FunctionError, FunctionResult};

/// Aggregate function evaluator.
///
/// Handles evaluation of aggregate functions like COUNT, SUM, AVG, MIN, MAX.
pub struct AggregateEvaluator;

impl AggregateEvaluator {
    /// Evaluate an aggregate expression over a set of bindings.
    pub fn evaluate(
        agg: &ast::AggregateExpression,
        contexts: &[EvalContext],
        graph: &Graph,
    ) -> FunctionResult<Value> {
        match agg.func {
            ast::AggregateFunction::Count => Self::count(contexts),
            ast::AggregateFunction::Sum => Self::sum(agg, contexts, graph),
        }
    }

    /// COUNT function - counts the number of matched entities.
    fn count(contexts: &[EvalContext]) -> FunctionResult<Value> {
        let count = contexts.len();
        Ok(Value::Number(count.into()))
    }

    /// SUM function - sums numeric property values.
    fn sum(
        agg: &ast::AggregateExpression,
        contexts: &[EvalContext],
        graph: &Graph,
    ) -> FunctionResult<Value> {
        let mut sum: i64 = 0;

        for context in contexts {
            if let Some(node_idx) = context.get_binding(&agg.variable) {
                let node = &graph.nodes[node_idx];

                let value = if let Some(ref prop) = agg.property {
                    node.get_property_as_i64(prop)
                } else {
                    None
                };

                if let Some(v) = value {
                    sum += v;
                }
            }
        }

        Ok(Value::Number(sum.into()))
    }

    /// Get the column name for an aggregate expression.
    pub fn column_name(agg: &ast::AggregateExpression) -> String {
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
}

/// Extension to add more aggregate functions.
impl AggregateEvaluator {
    /// AVG function - calculates the average of numeric property values.
    pub fn avg(
        _agg: &ast::AggregateExpression,
        _contexts: &[EvalContext],
        _graph: &Graph,
    ) -> FunctionResult<Value> {
        // TODO: Implement AVG
        Err(FunctionError::NotImplemented("AVG".to_string()))
    }

    /// MIN function - finds the minimum value.
    pub fn min(
        _agg: &ast::AggregateExpression,
        _contexts: &[EvalContext],
        _graph: &Graph,
    ) -> FunctionResult<Value> {
        // TODO: Implement MIN
        Err(FunctionError::NotImplemented("MIN".to_string()))
    }

    /// MAX function - finds the maximum value.
    pub fn max(
        _agg: &ast::AggregateExpression,
        _contexts: &[EvalContext],
        _graph: &Graph,
    ) -> FunctionResult<Value> {
        // TODO: Implement MAX
        Err(FunctionError::NotImplemented("MAX".to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::Node;
    use crate::parser::ast::AggregateFunction;
    use serde_json::json;

    fn create_test_graph() -> Graph {
        let mut graph = Graph::new();
        graph.add_node(Node::new(
            "1".to_string(),
            None,
            json!({"id": "1", "value": 10}),
        ));
        graph.add_node(Node::new(
            "2".to_string(),
            None,
            json!({"id": "2", "value": 20}),
        ));
        graph.add_node(Node::new(
            "3".to_string(),
            None,
            json!({"id": "3", "value": 30}),
        ));
        graph
    }

    fn create_test_contexts(graph: &Graph) -> Vec<EvalContext> {
        let mut contexts = Vec::new();
        for i in 0..graph.nodes.len() {
            let mut ctx = EvalContext::new();
            ctx.bind("n".to_string(), i);
            contexts.push(ctx);
        }
        contexts
    }

    #[test]
    fn test_count() {
        let graph = create_test_graph();
        let contexts = create_test_contexts(&graph);

        let agg = ast::AggregateExpression {
            func: AggregateFunction::Count,
            variable: "n".to_string(),
            property: None,
        };

        let result = AggregateEvaluator::evaluate(&agg, &contexts, &graph).unwrap();
        assert_eq!(result.as_i64(), Some(3));
    }

    #[test]
    fn test_sum() {
        let graph = create_test_graph();
        let contexts = create_test_contexts(&graph);

        let agg = ast::AggregateExpression {
            func: AggregateFunction::Sum,
            variable: "n".to_string(),
            property: Some("value".to_string()),
        };

        let result = AggregateEvaluator::evaluate(&agg, &contexts, &graph).unwrap();
        assert_eq!(result.as_i64(), Some(60)); // 10 + 20 + 30 = 60
    }

    #[test]
    fn test_column_name() {
        let agg_count = ast::AggregateExpression {
            func: AggregateFunction::Count,
            variable: "n".to_string(),
            property: None,
        };
        assert_eq!(AggregateEvaluator::column_name(&agg_count), "COUNT(n)");

        let agg_sum = ast::AggregateExpression {
            func: AggregateFunction::Sum,
            variable: "n".to_string(),
            property: Some("value".to_string()),
        };
        assert_eq!(AggregateEvaluator::column_name(&agg_sum), "SUM(n.value)");
    }
}
