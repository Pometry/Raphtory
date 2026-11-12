//! A walk of the SPARQL algebra of a query.
use spargebra::{
    algebra::{
        AggregateExpression, Expression, GraphPattern, OrderExpression, PropertyPathExpression,
    },
    Query,
};

#[derive(Clone, Copy)]
pub(crate) enum Node<'a> {
    Pattern(&'a GraphPattern),
    Expression(&'a Expression),
    Path(&'a PropertyPathExpression),
}

// iterative, so a query of any length needs little thread stack; children in algebra order
pub(crate) fn walk<'a>(query: &'a Query, mut visit: impl FnMut(Node<'a>)) {
    let (Query::Select { pattern, .. }
    | Query::Ask { pattern, .. }
    | Query::Construct { pattern, .. }
    | Query::Describe { pattern, .. }) = query;
    let mut stack = vec![Node::Pattern(pattern)];
    while let Some(node) = stack.pop() {
        visit(node);
        let first_child = stack.len();
        match node {
            Node::Pattern(pattern) => push_pattern_children(pattern, &mut stack),
            Node::Expression(expression) => push_expression_children(expression, &mut stack),
            Node::Path(path) => push_path_children(path, &mut stack),
        }
        // popped last first, so the first child is visited next
        stack[first_child..].reverse();
    }
}

fn push_pattern_children<'a>(pattern: &'a GraphPattern, stack: &mut Vec<Node<'a>>) {
    match pattern {
        GraphPattern::Bgp { .. } | GraphPattern::Values { .. } => {}
        GraphPattern::Path { path, .. } => stack.push(Node::Path(path)),
        GraphPattern::Join { left, right }
        | GraphPattern::Lateral { left, right }
        | GraphPattern::Union { left, right }
        | GraphPattern::Minus { left, right } => {
            stack.extend([Node::Pattern(left), Node::Pattern(right)])
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            stack.extend([Node::Pattern(left), Node::Pattern(right)]);
            stack.extend(expression.iter().map(Node::Expression));
        }
        GraphPattern::Filter { expr, inner } => {
            stack.extend([Node::Expression(expr), Node::Pattern(inner)])
        }
        GraphPattern::Extend {
            inner, expression, ..
        } => stack.extend([Node::Pattern(inner), Node::Expression(expression)]),
        GraphPattern::OrderBy { inner, expression } => {
            stack.push(Node::Pattern(inner));
            stack.extend(expression.iter().map(|order| match order {
                OrderExpression::Asc(e) | OrderExpression::Desc(e) => Node::Expression(e),
            }));
        }
        GraphPattern::Group {
            inner, aggregates, ..
        } => {
            stack.push(Node::Pattern(inner));
            stack.extend(
                aggregates
                    .iter()
                    .filter_map(|(_, aggregate)| match aggregate {
                        AggregateExpression::CountSolutions { .. } => None,
                        AggregateExpression::FunctionCall { expr, .. } => {
                            Some(Node::Expression(expr))
                        }
                    }),
            );
        }
        GraphPattern::Graph { inner, .. }
        | GraphPattern::Service { inner, .. }
        | GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. } => stack.push(Node::Pattern(inner)),
    }
}

fn push_expression_children<'a>(expression: &'a Expression, stack: &mut Vec<Node<'a>>) {
    match expression {
        Expression::NamedNode(_)
        | Expression::Literal(_)
        | Expression::Variable(_)
        | Expression::Bound(_) => {}
        Expression::Or(a, b)
        | Expression::And(a, b)
        | Expression::Equal(a, b)
        | Expression::SameTerm(a, b)
        | Expression::Greater(a, b)
        | Expression::GreaterOrEqual(a, b)
        | Expression::Less(a, b)
        | Expression::LessOrEqual(a, b)
        | Expression::Add(a, b)
        | Expression::Subtract(a, b)
        | Expression::Multiply(a, b)
        | Expression::Divide(a, b) => stack.extend([Node::Expression(a), Node::Expression(b)]),
        Expression::UnaryPlus(a) | Expression::UnaryMinus(a) | Expression::Not(a) => {
            stack.push(Node::Expression(a))
        }
        Expression::In(a, list) => {
            stack.push(Node::Expression(a));
            stack.extend(list.iter().map(Node::Expression));
        }
        Expression::If(a, b, c) => stack.extend([
            Node::Expression(a),
            Node::Expression(b),
            Node::Expression(c),
        ]),
        Expression::Coalesce(list) | Expression::FunctionCall(_, list) => {
            stack.extend(list.iter().map(Node::Expression))
        }
        Expression::Exists(pattern) => stack.push(Node::Pattern(pattern)),
    }
}

fn push_path_children<'a>(path: &'a PropertyPathExpression, stack: &mut Vec<Node<'a>>) {
    match path {
        PropertyPathExpression::NamedNode(_) | PropertyPathExpression::NegatedPropertySet(_) => {}
        PropertyPathExpression::Reverse(p)
        | PropertyPathExpression::ZeroOrMore(p)
        | PropertyPathExpression::OneOrMore(p)
        | PropertyPathExpression::ZeroOrOne(p) => stack.push(Node::Path(p)),
        PropertyPathExpression::Sequence(a, b) | PropertyPathExpression::Alternative(a, b) => {
            stack.extend([Node::Path(a), Node::Path(b)])
        }
    }
}
