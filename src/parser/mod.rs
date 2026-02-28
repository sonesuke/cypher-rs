pub mod ast;

use anyhow::{anyhow, Result};
use pest::iterators::Pair;
use pest::Parser;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "parser/cypher.pest"]
struct CypherParser;

pub fn parse_query(query_str: &str) -> Result<ast::Query> {
    let pairs =
        CypherParser::parse(Rule::query, query_str).map_err(|e| anyhow!("Parse error: {}", e))?;

    let mut match_clause = None;
    let mut where_clause = None;
    let mut return_clause = None;

    for pair in pairs.into_iter().next().unwrap().into_inner() {
        match pair.as_rule() {
            Rule::MATCH => {}
            Rule::pattern => {
                match_clause = Some(parse_match_clause(pair)?);
            }
            Rule::WHERE => {}
            Rule::where_clause => {
                where_clause = Some(parse_where_clause(pair)?);
            }
            Rule::RETURN => {}
            Rule::return_clause => {
                return_clause = Some(parse_return_clause(pair)?);
            }
            _ => {}
        }
    }

    Ok(ast::Query {
        match_clause: match_clause.ok_or_else(|| anyhow!("Missing MATCH clause"))?,
        where_clause,
        return_clause: return_clause.ok_or_else(|| anyhow!("Missing RETURN clause"))?,
    })
}

fn parse_match_clause(pair: Pair<Rule>) -> Result<ast::MatchClause> {
    let mut patterns = Vec::new();
    for p in pair.into_inner() {
        if p.as_rule() == Rule::pattern_part {
            patterns.push(parse_pattern_part(p)?);
        }
    }
    Ok(ast::MatchClause { patterns })
}

fn parse_pattern_part(pair: Pair<Rule>) -> Result<ast::PatternPart> {
    let mut chains = Vec::new();
    let mut inner = pair.into_inner();

    // First element is always a node pattern
    let first_node = parse_node_pattern(inner.next().unwrap())?;
    chains.push(ast::PatternChain::Node(first_node));

    while let Some(rel_pair) = inner.next() {
        if rel_pair.as_rule() == Rule::relationship_pattern {
            let rel_pattern = parse_relationship_pattern(rel_pair)?;
            let next_node_pair = inner
                .next()
                .ok_or_else(|| anyhow!("Missing node after relationship"))?;
            let next_node = parse_node_pattern(next_node_pair)?;
            chains.push(ast::PatternChain::Relationship(rel_pattern, next_node));
        }
    }

    Ok(ast::PatternPart { chains })
}

fn parse_node_pattern(pair: Pair<Rule>) -> Result<ast::NodePattern> {
    let mut variable = None;
    let mut labels = Vec::new();

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::variable => variable = Some(p.as_str().to_string()),
            Rule::node_labels => {
                for l in p.into_inner() {
                    if l.as_rule() == Rule::node_label {
                        let label_name = l.into_inner().next().unwrap().as_str().to_string();
                        labels.push(label_name);
                    }
                }
            }
            _ => {}
        }
    }
    Ok(ast::NodePattern { variable, labels })
}

fn parse_relationship_pattern(pair: Pair<Rule>) -> Result<ast::RelationshipPattern> {
    let s = pair.as_str();
    let direction = if s.starts_with('<') {
        ast::Direction::Left
    } else if s.ends_with('>') {
        ast::Direction::Right
    } else {
        ast::Direction::Both
    };

    let mut variable = None;
    let mut rel_type = None;
    let mut range = None;

    for p in pair.into_inner() {
        if p.as_rule() == Rule::relationship_detail {
            for d in p.into_inner() {
                match d.as_rule() {
                    Rule::variable => variable = Some(d.as_str().to_string()),
                    Rule::relationship_types => {
                        // For simplicity, take the first type
                        let type_pair = d.into_inner().next().unwrap();
                        rel_type = Some(type_pair.as_str().to_string());
                    }
                    Rule::range_literal => {
                        range = parse_range_literal(d).ok();
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(ast::RelationshipPattern {
        variable,
        rel_type,
        range,
        direction,
    })
}

fn parse_where_clause(pair: Pair<Rule>) -> Result<ast::WhereClause> {
    let expr_pair = pair.into_inner().next().unwrap();
    let expression = parse_expression(expr_pair)?;
    Ok(ast::WhereClause { expression })
}

fn parse_return_clause(pair: Pair<Rule>) -> Result<ast::ReturnClause> {
    let mut items = Vec::new();
    for p in pair.into_inner() {
        if p.as_rule() == Rule::return_item {
            items.push(parse_return_item(p)?);
        }
    }
    Ok(ast::ReturnClause { items })
}

fn parse_return_item(pair: Pair<Rule>) -> Result<ast::ReturnItem> {
    let mut inner = pair.into_inner();
    let expr_pair = inner.next().unwrap();
    let expression = parse_expression(expr_pair)?;

    let mut alias = None;
    for p in inner {
        match p.as_rule() {
            Rule::variable => alias = Some(p.as_str().to_string()),
            Rule::AS => {}
            _ => {}
        }
    }

    Ok(ast::ReturnItem { expression, alias })
}

fn parse_range_literal(pair: Pair<Rule>) -> Result<ast::Range> {
    let mut start = None;
    let mut end = None;

    for p in pair.into_inner() {
        if p.as_rule() == Rule::range_quantifier {
            let inner = p.clone().into_inner().next().unwrap();
            match inner.as_rule() {
                Rule::range_exact => {
                    let val = inner.as_str().parse::<usize>().ok();
                    start = val;
                    end = val;
                }
                _ => {
                    for q in p.into_inner() {
                        match q.as_rule() {
                            Rule::range_start => {
                                start = q.as_str().parse::<usize>().ok();
                            }
                            Rule::range_end => {
                                end = q.as_str().parse::<usize>().ok();
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
    }

    Ok(ast::Range { start, end })
}

fn parse_expression(pair: Pair<Rule>) -> Result<ast::Expression> {
    let or_expr = pair.into_inner().next().unwrap();
    let mut or_parts = Vec::new();

    for and_expr in or_expr.into_inner() {
        if and_expr.as_rule() == Rule::and_expression {
            let mut and_parts = Vec::new();
            for comp_expr in and_expr.into_inner() {
                if comp_expr.as_rule() == Rule::comparison_expression {
                    and_parts.push(parse_comparison_expression(comp_expr)?);
                }
            }
            if and_parts.len() == 1 {
                or_parts.push(and_parts[0].clone());
            } else {
                or_parts.push(ast::Expression::And(and_parts));
            }
        }
    }

    if or_parts.len() == 1 {
        Ok(or_parts[0].clone())
    } else {
        Ok(ast::Expression::Or(or_parts))
    }
}

fn parse_comparison_expression(pair: Pair<Rule>) -> Result<ast::Expression> {
    let mut inner = pair.into_inner();
    let left_pair = inner.next().unwrap();

    // Check if it's an aggregate call
    if left_pair.as_rule() == Rule::aggregate_call {
        return parse_aggregate_call(left_pair);
    }

    let left = parse_property_or_variable(left_pair)?;

    if let Some(op_pair) = inner.next() {
        let operator = match op_pair.as_str().to_uppercase().as_str() {
            "=" => ast::ComparisonOperator::Eq,
            "<>" => ast::ComparisonOperator::NotEq,
            "<" => ast::ComparisonOperator::Lt,
            ">" => ast::ComparisonOperator::Gt,
            "<=" => ast::ComparisonOperator::LtEq,
            ">=" => ast::ComparisonOperator::GtEq,
            "CONTAINS" => ast::ComparisonOperator::Contains,
            _ => unreachable!(),
        };

        let right_pair = inner.next().unwrap();
        let right = parse_term(right_pair)?;

        Ok(ast::Expression::Comparison(ast::Comparison {
            left,
            operator: Some(operator),
            right: Some(right),
        }))
    } else {
        Ok(ast::Expression::Comparison(ast::Comparison {
            left,
            operator: None,
            right: None,
        }))
    }
}

fn parse_aggregate_call(pair: Pair<Rule>) -> Result<ast::Expression> {
    let mut inner = pair.into_inner();
    let func_pair = inner.next().unwrap();
    let func_str = func_pair.as_str().to_uppercase();
    let func = match func_str.as_str() {
        "COUNT" => ast::AggregateFunction::Count,
        "SUM" => ast::AggregateFunction::Sum,
        _ => return Err(anyhow!("Unknown aggregate function: {}", func_str)),
    };

    let variable_pair = inner
        .find(|p| p.as_rule() == Rule::variable)
        .ok_or_else(|| anyhow!("Missing variable in aggregate function"))?;
    let variable = variable_pair.as_str().to_string();

    let property = inner
        .find(|p| p.as_rule() == Rule::property_name)
        .map(|p| p.as_str().to_string());

    Ok(ast::Expression::Aggregate(ast::AggregateExpression {
        func,
        variable,
        property,
    }))
}

fn parse_property_or_variable(pair: Pair<Rule>) -> Result<ast::PropertyOrVariable> {
    let mut inner = pair.into_inner();
    let variable = inner.next().unwrap().as_str().to_string();
    let property = inner.next().map(|p| p.as_str().to_string());
    Ok(ast::PropertyOrVariable { variable, property })
}

fn parse_term(pair: Pair<Rule>) -> Result<ast::Term> {
    let inner = pair.into_inner().next().unwrap();
    match inner.as_rule() {
        Rule::literal => {
            let lit = inner.into_inner().next().unwrap();
            match lit.as_rule() {
                Rule::string_literal => {
                    let s = lit.as_str();
                    Ok(ast::Term::Literal(ast::Literal::String(
                        s[1..s.len() - 1].to_string(),
                    )))
                }
                Rule::number_literal => Ok(ast::Term::Literal(ast::Literal::Number(
                    lit.as_str().parse().unwrap(),
                ))),
                _ => unreachable!(),
            }
        }
        Rule::property_or_variable => Ok(ast::Term::PropertyOrVariable(
            parse_property_or_variable(inner)?,
        )),
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_basic() {
        let q = "MATCH (n:UC) RETURN n.id";
        let parsed = parse_query(q).unwrap();
        assert_eq!(parsed.match_clause.patterns.len(), 1);
        assert_eq!(parsed.return_clause.items.len(), 1);
    }

    #[test]
    fn test_parse_relationship() {
        let q = "MATCH (n:UC)-[r:realized_by]->(m:FR) RETURN n, m";
        let parsed = parse_query(q).unwrap();
        assert_eq!(parsed.match_clause.patterns.len(), 1);
    }

    #[test]
    fn test_parse_where() {
        let q = "MATCH (n) WHERE n.id = \"UC_001\" RETURN n";
        let parsed = parse_query(q).unwrap();
        assert!(parsed.where_clause.is_some());
    }

    #[test]
    fn test_parse_alias() {
        let q = "MATCH (n) RETURN n.id AS identifier";
        let parsed = parse_query(q).unwrap();
        let item = &parsed.return_clause.items[0];
        assert_eq!(item.alias, Some("identifier".to_string()));
    }

    #[test]
    fn test_parse_count() {
        let q = "MATCH (n) RETURN COUNT(n)";
        let parsed = parse_query(q).unwrap();
        assert_eq!(parsed.return_clause.items.len(), 1);
    }

    #[test]
    fn test_parse_sum() {
        let q = "MATCH (n) RETURN SUM(n.value)";
        let parsed = parse_query(q).unwrap();
        assert_eq!(parsed.return_clause.items.len(), 1);
    }
}
