//! Function evaluation for Cypher queries.
//!
//! This module provides implementations for various Cypher functions
//! including aggregate functions, string functions, and mathematical functions.

pub mod aggregate;

use std::collections::HashMap;

/// Result type for function evaluation.
pub type FunctionResult<T> = std::result::Result<T, FunctionError>;

/// Errors that can occur during function evaluation.
#[derive(Debug, thiserror::Error)]
pub enum FunctionError {
    #[error("Function not implemented: {0}")]
    NotImplemented(String),

    #[error("Invalid arguments for function '{0}': {1}")]
    InvalidArguments(String, String),

    #[error("Type error in function '{0}': {1}")]
    TypeError(String, String),

    #[error("Property not found: {0}")]
    PropertyNotFound(String),

    #[error("Variable not bound: {0}")]
    VariableNotBound(String),
}

/// Evaluation context for function execution.
///
/// Contains variable bindings and other context information needed
/// during query execution.
#[derive(Debug, Clone, Default)]
pub struct EvalContext {
    /// Variable bindings to node indices
    bindings: HashMap<String, usize>,
}

impl EvalContext {
    /// Create a new evaluation context.
    pub fn new() -> Self {
        Self::default()
    }

    /// Bind a variable to a node index.
    pub fn bind(&mut self, variable: String, node_idx: usize) {
        self.bindings.insert(variable, node_idx);
    }

    /// Get the binding for a variable.
    pub fn get_binding(&self, variable: &str) -> Option<usize> {
        self.bindings.get(variable).copied()
    }

    /// Check if a variable is bound.
    pub fn has_binding(&self, variable: &str) -> bool {
        self.bindings.contains_key(variable)
    }

    /// Get all bindings.
    pub fn bindings(&self) -> &HashMap<String, usize> {
        &self.bindings
    }

    /// Create a context from a bindings map.
    pub fn from_bindings(bindings: HashMap<String, usize>) -> Self {
        Self { bindings }
    }

    /// Clone with updated binding.
    pub fn with_binding(&self, variable: String, node_idx: usize) -> Self {
        let mut new_ctx = self.clone();
        new_ctx.bind(variable, node_idx);
        new_ctx
    }
}

/// Context for evaluating expressions during query execution.
///
/// This provides access to the graph and variable bindings needed
/// to evaluate expressions and function calls.
pub struct ExpressionContext<'a> {
    /// The graph being queried
    pub graph: &'a crate::graph::Graph,
    /// Current variable bindings
    pub bindings: &'a HashMap<String, crate::engine::EntityId>,
}

// Re-export aggregate evaluator
pub use aggregate::AggregateEvaluator;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_eval_context() {
        let mut ctx = EvalContext::new();
        assert!(!ctx.has_binding("n"));

        ctx.bind("n".to_string(), 0);
        assert!(ctx.has_binding("n"));
        assert_eq!(ctx.get_binding("n"), Some(0));
    }

    #[test]
    fn test_eval_context_with_binding() {
        let mut ctx1 = EvalContext::new();
        ctx1.bind("n".to_string(), 0);

        let ctx2 = ctx1.with_binding("m".to_string(), 1);

        assert!(ctx2.has_binding("n"));
        assert!(ctx2.has_binding("m"));
        assert_eq!(ctx2.get_binding("n"), Some(0));
        assert_eq!(ctx2.get_binding("m"), Some(1));
    }
}
