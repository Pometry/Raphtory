//! The limits of [`SparqlOptions`](crate::SparqlOptions).
use crate::{
    algebra::{walk, Node},
    RdfError,
};
use spargebra::{
    algebra::{GraphPattern, PropertyPathExpression},
    Query,
};
use std::io::{self, Write};

pub(crate) fn check_query_length(query: &str, max: Option<usize>) -> Result<(), RdfError> {
    match max.filter(|&max| query.len() > max) {
        Some(max) => Err(RdfError::QueryTooLong {
            length: query.len(),
            max,
        }),
        None => Ok(()),
    }
}

pub(crate) const VALUES_ROWS_PER_PATTERN: usize = 100;

pub(crate) fn count_triple_patterns(query: &Query) -> usize {
    let mut count = 0usize;
    walk(query, |node| {
        let weight = match node {
            Node::Pattern(GraphPattern::Bgp { patterns }) => patterns.len(),
            Node::Pattern(GraphPattern::Extend { .. }) => 1,
            Node::Pattern(GraphPattern::Values {
                variables,
                bindings,
            }) => 1usize
                .saturating_add(variables.len())
                .saturating_add(bindings.len() / VALUES_ROWS_PER_PATTERN),
            Node::Path(
                PropertyPathExpression::NamedNode(_)
                | PropertyPathExpression::NegatedPropertySet(_),
            ) => 1,
            _ => 0,
        };
        count = count.saturating_add(weight);
    });
    count
}

pub(crate) fn check_triple_patterns(query: &Query, max: Option<usize>) -> Result<(), RdfError> {
    let Some(max) = max else {
        return Ok(());
    };
    let count = count_triple_patterns(query);
    if count > max {
        Err(RdfError::TooManyPatterns { count, max })
    } else {
        Ok(())
    }
}

pub(crate) struct Capped<W> {
    inner: W,
    max: Option<usize>,
    written: usize,
    exceeded: bool,
}

impl<W: Write> Capped<W> {
    pub(crate) fn new(inner: W, max: Option<usize>) -> Self {
        Self {
            inner,
            max,
            written: 0,
            exceeded: false,
        }
    }

    // a refused write surfaces as whatever error the serializer made of it
    pub(crate) fn check<T>(&self, result: Result<T, RdfError>) -> Result<T, RdfError> {
        match self.max {
            Some(max) if self.exceeded => Err(RdfError::ResultsTooLarge { max }),
            _ => result,
        }
    }
}

impl<W: Write> Write for Capped<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self
            .max
            .is_some_and(|max| self.written.saturating_add(buf.len()) > max)
        {
            self.exceeded = true;
            return Err(io::Error::other("the output is larger than its limit"));
        }
        let written = self.inner.write(buf)?;
        self.written += written;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
