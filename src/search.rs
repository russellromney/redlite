//! RediSearch query parser and FTS5 query translator
//!
//! This module parses RediSearch query syntax and translates it to SQLite FTS5 MATCH expressions.
//!
//! ## Supported Query Syntax
//!
//! - Simple terms: `hello world` (implicit AND)
//! - OR operator: `hello | world`
//! - NOT operator: `-word` or `!word`
//! - Phrase matching: `"exact phrase"`
//! - Prefix search: `hel*`
//! - Field-scoped text: `@title:hello`
//! - Field-scoped phrase: `@title:"hello world"`
//! - Numeric range: `@price:[10 100]` or `@price:[(10 (100]` for exclusive bounds
//! - Tag exact match: `@category:{electronics|books}`
//! - Grouping: `(term1 | term2) term3`
//!
//! ## FTS5 Translation
//!
//! RediSearch syntax is translated to FTS5 MATCH expressions:
//! - `word1 word2` -> `word1 AND word2`
//! - `word1 | word2` -> `word1 OR word2`
//! - `-word` -> `NOT word`
//! - `"phrase"` -> `"phrase"`
//! - `word*` -> `word*`
//! - `@field:term` -> `field:term`

/// Parsed query component
#[derive(Debug, Clone, PartialEq)]
pub enum QueryExpr {
    /// Simple term (word)
    Term(String),
    /// Prefix search (word*)
    Prefix(String),
    /// Exact phrase ("words here")
    Phrase(String),
    /// Field-scoped query (@field:expr)
    FieldText { field: String, expr: Box<QueryExpr> },
    /// Numeric range query (@field:[min max])
    NumericRange {
        field: String,
        min: NumericBound,
        max: NumericBound,
    },
    /// Tag exact match (@field:{tag1|tag2})
    TagMatch { field: String, tags: Vec<String> },
    /// AND of multiple expressions
    And(Vec<QueryExpr>),
    /// OR of multiple expressions
    Or(Vec<QueryExpr>),
    /// NOT expression
    Not(Box<QueryExpr>),
    /// Match all documents (wildcard *)
    MatchAll,
}

/// Numeric range bound
#[derive(Debug, Clone, PartialEq)]
pub enum NumericBound {
    /// Inclusive bound
    Inclusive(f64),
    /// Exclusive bound
    Exclusive(f64),
    /// Unbounded (negative or positive infinity)
    Unbounded,
}

impl NumericBound {
    pub fn value(&self) -> Option<f64> {
        match self {
            NumericBound::Inclusive(v) | NumericBound::Exclusive(v) => Some(*v),
            NumericBound::Unbounded => None,
        }
    }
}

/// Query parsing result with text query and filter components
#[derive(Debug, Clone)]
pub struct ParsedQuery {
    /// FTS5 MATCH expression for text search
    pub fts_query: Option<String>,
    /// Numeric range filters (field -> (min, max, min_exclusive, max_exclusive))
    pub numeric_filters: Vec<NumericFilter>,
    /// Tag filters (field -> tags)
    pub tag_filters: Vec<TagFilter>,
    /// Fields to search in (from @field: syntax), None = all fields
    pub search_fields: Vec<String>,
}

/// Numeric range filter
#[derive(Debug, Clone)]
pub struct NumericFilter {
    pub field: String,
    pub min: NumericBound,
    pub max: NumericBound,
}

/// Tag filter
#[derive(Debug, Clone)]
pub struct TagFilter {
    pub field: String,
    pub tags: Vec<String>,
}

impl ParsedQuery {
    pub fn new() -> Self {
        Self {
            fts_query: None,
            numeric_filters: Vec::new(),
            tag_filters: Vec::new(),
            search_fields: Vec::new(),
        }
    }
}

/// Query parser for RediSearch syntax
pub struct QueryParser<'a> {
    input: &'a str,
    pos: usize,
    verbatim: bool,
}

impl<'a> QueryParser<'a> {
    pub fn new(input: &'a str, verbatim: bool) -> Self {
        Self {
            input,
            pos: 0,
            verbatim,
        }
    }

    /// Parse the query and return a ParsedQuery with FTS5 expression and filters
    pub fn parse(&mut self) -> Result<ParsedQuery, String> {
        let expr = self.parse_or()?;
        let mut result = ParsedQuery::new();
        self.extract_components(&expr, &mut result);
        Ok(result)
    }

    /// Parse the query into a QueryExpr AST
    pub fn parse_expr(&mut self) -> Result<QueryExpr, String> {
        self.parse_or()
    }

    fn extract_components(&self, expr: &QueryExpr, result: &mut ParsedQuery) {
        match expr {
            QueryExpr::NumericRange { field, min, max } => {
                result.numeric_filters.push(NumericFilter {
                    field: field.clone(),
                    min: min.clone(),
                    max: max.clone(),
                });
            }
            QueryExpr::TagMatch { field, tags } => {
                result.tag_filters.push(TagFilter {
                    field: field.clone(),
                    tags: tags.clone(),
                });
            }
            QueryExpr::FieldText { field, expr } => {
                if !result.search_fields.contains(field) {
                    result.search_fields.push(field.clone());
                }
                // Generate FTS5 for the field-scoped query
                let fts = self.expr_to_fts5(expr, Some(field));
                self.append_fts_query(result, &fts, "AND");
            }
            QueryExpr::And(exprs) => {
                for e in exprs {
                    self.extract_components(e, result);
                }
            }
            QueryExpr::Or(exprs) => {
                // For OR at top level, we need to handle differently
                let mut fts_parts = Vec::new();
                for e in exprs {
                    match e {
                        QueryExpr::NumericRange { .. } | QueryExpr::TagMatch { .. } => {
                            self.extract_components(e, result);
                        }
                        _ => {
                            let fts = self.expr_to_fts5(e, None);
                            if !fts.is_empty() {
                                fts_parts.push(fts);
                            }
                        }
                    }
                }
                if !fts_parts.is_empty() {
                    let combined = fts_parts.join(" OR ");
                    self.append_fts_query(result, &format!("({})", combined), "AND");
                }
            }
            QueryExpr::Not(inner) => {
                let fts = self.expr_to_fts5(inner, None);
                if !fts.is_empty() {
                    self.append_fts_query(result, &format!("NOT {}", fts), "AND");
                }
            }
            QueryExpr::MatchAll => {
                // Match all - no FTS query needed
            }
            _ => {
                let fts = self.expr_to_fts5(expr, None);
                self.append_fts_query(result, &fts, "AND");
            }
        }
    }

    fn append_fts_query(&self, result: &mut ParsedQuery, fts: &str, op: &str) {
        if fts.is_empty() {
            return;
        }
        match &mut result.fts_query {
            Some(existing) => {
                *existing = format!("{} {} {}", existing, op, fts);
            }
            None => {
                result.fts_query = Some(fts.to_string());
            }
        }
    }

    fn expr_to_fts5(&self, expr: &QueryExpr, field: Option<&str>) -> String {
        match expr {
            QueryExpr::Term(t) => {
                let escaped = escape_fts5_term(t);
                match field {
                    Some(f) => format!("\"{}\":{}", f, escaped),
                    None => escaped,
                }
            }
            QueryExpr::Prefix(p) => {
                let escaped = escape_fts5_term(p);
                match field {
                    Some(f) => format!("\"{}\":{}*", f, escaped),
                    None => format!("{}*", escaped),
                }
            }
            QueryExpr::Phrase(p) => {
                let escaped = escape_fts5_phrase(p);
                match field {
                    Some(f) => format!("\"{}\":\"{}\"", f, escaped),
                    None => format!("\"{}\"", escaped),
                }
            }
            QueryExpr::FieldText { field: f, expr } => self.expr_to_fts5(expr, Some(f)),
            QueryExpr::And(exprs) => {
                let parts: Vec<String> = exprs
                    .iter()
                    .map(|e| self.expr_to_fts5(e, field))
                    .filter(|s| !s.is_empty())
                    .collect();
                if parts.is_empty() {
                    String::new()
                } else if parts.len() == 1 {
                    parts.into_iter().next().unwrap()
                } else {
                    format!("({})", parts.join(" AND "))
                }
            }
            QueryExpr::Or(exprs) => {
                let parts: Vec<String> = exprs
                    .iter()
                    .map(|e| self.expr_to_fts5(e, field))
                    .filter(|s| !s.is_empty())
                    .collect();
                if parts.is_empty() {
                    String::new()
                } else if parts.len() == 1 {
                    parts.into_iter().next().unwrap()
                } else {
                    format!("({})", parts.join(" OR "))
                }
            }
            QueryExpr::Not(inner) => {
                let inner_fts = self.expr_to_fts5(inner, field);
                if inner_fts.is_empty() {
                    String::new()
                } else {
                    format!("NOT {}", inner_fts)
                }
            }
            QueryExpr::NumericRange { .. } | QueryExpr::TagMatch { .. } => {
                // These are handled as SQL filters, not FTS5
                String::new()
            }
            QueryExpr::MatchAll => String::new(),
        }
    }

    // Parsing methods

    fn parse_or(&mut self) -> Result<QueryExpr, String> {
        let mut left = self.parse_and()?;

        while self.skip_whitespace() && self.peek() == Some('|') {
            self.advance(); // consume '|'
            self.skip_whitespace();
            let right = self.parse_and()?;

            left = match left {
                QueryExpr::Or(mut exprs) => {
                    exprs.push(right);
                    QueryExpr::Or(exprs)
                }
                _ => QueryExpr::Or(vec![left, right]),
            };
        }

        Ok(left)
    }

    fn parse_and(&mut self) -> Result<QueryExpr, String> {
        let mut exprs = Vec::new();

        loop {
            self.skip_whitespace();

            // Check for end of input or closing paren
            match self.peek() {
                None | Some(')') | Some('|') => break,
                _ => {}
            }

            let expr = self.parse_unary()?;
            exprs.push(expr);
        }

        if exprs.is_empty() {
            Ok(QueryExpr::MatchAll)
        } else if exprs.len() == 1 {
            Ok(exprs.remove(0))
        } else {
            Ok(QueryExpr::And(exprs))
        }
    }

    fn parse_unary(&mut self) -> Result<QueryExpr, String> {
        self.skip_whitespace();

        // Check for NOT operators
        if self.peek() == Some('-') || self.peek() == Some('!') {
            self.advance();
            let expr = self.parse_primary()?;
            return Ok(QueryExpr::Not(Box::new(expr)));
        }

        self.parse_primary()
    }

    fn parse_primary(&mut self) -> Result<QueryExpr, String> {
        self.skip_whitespace();

        match self.peek() {
            Some('(') => self.parse_group(),
            Some('"') => self.parse_phrase(),
            Some('@') => self.parse_field_query(),
            Some('*') => {
                self.advance();
                Ok(QueryExpr::MatchAll)
            }
            Some(_) => self.parse_term(),
            None => Ok(QueryExpr::MatchAll),
        }
    }

    fn parse_group(&mut self) -> Result<QueryExpr, String> {
        self.expect('(')?;
        let expr = self.parse_or()?;
        self.skip_whitespace();
        self.expect(')')?;
        Ok(expr)
    }

    fn parse_phrase(&mut self) -> Result<QueryExpr, String> {
        self.expect('"')?;
        let start = self.pos;

        while let Some(c) = self.peek() {
            if c == '"' {
                break;
            }
            if c == '\\' {
                self.advance();
                self.advance(); // Skip escaped char
            } else {
                self.advance();
            }
        }

        let phrase = self.input[start..self.pos].to_string();
        self.expect('"')?;

        Ok(QueryExpr::Phrase(phrase))
    }

    fn parse_field_query(&mut self) -> Result<QueryExpr, String> {
        self.expect('@')?;

        // Parse field name
        let field = self.parse_identifier()?;
        self.expect(':')?;

        self.skip_whitespace();

        // Check what follows the colon
        match self.peek() {
            Some('[') => self.parse_numeric_range(&field),
            Some('{') => self.parse_tag_match(&field),
            Some('"') => {
                let phrase = self.parse_phrase()?;
                Ok(QueryExpr::FieldText {
                    field,
                    expr: Box::new(phrase),
                })
            }
            Some('(') => {
                let group = self.parse_group()?;
                Ok(QueryExpr::FieldText {
                    field,
                    expr: Box::new(group),
                })
            }
            _ => {
                let term = self.parse_term()?;
                Ok(QueryExpr::FieldText {
                    field,
                    expr: Box::new(term),
                })
            }
        }
    }

    fn parse_numeric_range(&mut self, field: &str) -> Result<QueryExpr, String> {
        self.expect('[')?;
        self.skip_whitespace();

        // Parse min bound
        let min = self.parse_numeric_bound(true)?;

        self.skip_whitespace();

        // Parse max bound
        let max = self.parse_numeric_bound(false)?;

        self.skip_whitespace();
        self.expect(']')?;

        Ok(QueryExpr::NumericRange {
            field: field.to_string(),
            min,
            max,
        })
    }

    fn parse_numeric_bound(&mut self, is_min: bool) -> Result<NumericBound, String> {
        self.skip_whitespace();

        // Check for exclusive bound marker
        let exclusive = if self.peek() == Some('(') {
            self.advance();
            true
        } else {
            false
        };

        self.skip_whitespace();

        // Check for infinity markers
        match self.peek() {
            Some('-') if is_min => {
                // Could be -inf or negative number
                let start = self.pos;
                self.advance();
                if self.input[self.pos..].starts_with("inf") {
                    self.pos += 3;
                    return Ok(NumericBound::Unbounded);
                }
                // It's a negative number, continue parsing
                self.pos = start;
            }
            Some('+') if !is_min => {
                self.advance();
                if self.input[self.pos..].starts_with("inf") {
                    self.pos += 3;
                    return Ok(NumericBound::Unbounded);
                }
            }
            _ => {}
        }

        // Parse number
        let start = self.pos;
        if self.peek() == Some('-') || self.peek() == Some('+') {
            self.advance();
        }
        while let Some(c) = self.peek() {
            if c.is_ascii_digit() || c == '.' {
                self.advance();
            } else {
                break;
            }
        }

        let num_str = &self.input[start..self.pos];
        if num_str.is_empty() || num_str == "-" || num_str == "+" {
            return Err("Expected number in range".to_string());
        }

        let value: f64 = num_str
            .parse()
            .map_err(|_| format!("Invalid number: {}", num_str))?;

        if exclusive {
            Ok(NumericBound::Exclusive(value))
        } else {
            Ok(NumericBound::Inclusive(value))
        }
    }

    fn parse_tag_match(&mut self, field: &str) -> Result<QueryExpr, String> {
        self.expect('{')?;

        let mut tags = Vec::new();

        loop {
            self.skip_whitespace();

            // Parse tag value (can be quoted or unquoted)
            let tag = if self.peek() == Some('"') {
                self.expect('"')?;
                let start = self.pos;
                while let Some(c) = self.peek() {
                    if c == '"' {
                        break;
                    }
                    self.advance();
                }
                let tag = self.input[start..self.pos].to_string();
                self.expect('"')?;
                tag
            } else {
                let start = self.pos;
                while let Some(c) = self.peek() {
                    if c == '|' || c == '}' || c.is_whitespace() {
                        break;
                    }
                    self.advance();
                }
                self.input[start..self.pos].to_string()
            };

            if !tag.is_empty() {
                tags.push(tag);
            }

            self.skip_whitespace();

            match self.peek() {
                Some('|') => {
                    self.advance();
                    continue;
                }
                Some('}') => {
                    self.advance();
                    break;
                }
                _ => break,
            }
        }

        Ok(QueryExpr::TagMatch {
            field: field.to_string(),
            tags,
        })
    }

    fn parse_term(&mut self) -> Result<QueryExpr, String> {
        let start = self.pos;

        while let Some(c) = self.peek() {
            if c.is_whitespace() || c == '|' || c == ')' || c == '(' || c == '"' {
                break;
            }
            self.advance();
        }

        let term = self.input[start..self.pos].to_string();

        if term.is_empty() {
            return Err("Expected term".to_string());
        }

        // Check for prefix search
        if term.ends_with('*') {
            let prefix = term[..term.len() - 1].to_string();
            if prefix.is_empty() {
                return Ok(QueryExpr::MatchAll);
            }
            return Ok(QueryExpr::Prefix(prefix));
        }

        Ok(QueryExpr::Term(term))
    }

    fn parse_identifier(&mut self) -> Result<String, String> {
        let start = self.pos;

        while let Some(c) = self.peek() {
            if c.is_alphanumeric() || c == '_' {
                self.advance();
            } else {
                break;
            }
        }

        let ident = self.input[start..self.pos].to_string();
        if ident.is_empty() {
            return Err("Expected identifier".to_string());
        }

        Ok(ident)
    }

    // Helper methods

    fn peek(&self) -> Option<char> {
        self.input[self.pos..].chars().next()
    }

    fn advance(&mut self) {
        if let Some(c) = self.peek() {
            self.pos += c.len_utf8();
        }
    }

    fn skip_whitespace(&mut self) -> bool {
        let start = self.pos;
        while let Some(c) = self.peek() {
            if c.is_whitespace() {
                self.advance();
            } else {
                break;
            }
        }
        self.pos > start || self.peek().is_some()
    }

    fn expect(&mut self, expected: char) -> Result<(), String> {
        match self.peek() {
            Some(c) if c == expected => {
                self.advance();
                Ok(())
            }
            Some(c) => Err(format!("Expected '{}', found '{}'", expected, c)),
            None => Err(format!("Expected '{}', found end of input", expected)),
        }
    }
}

/// Escape a term for FTS5 query
fn escape_fts5_term(term: &str) -> String {
    // FTS5 special characters that need escaping: " ( ) * : ^
    let mut result = String::with_capacity(term.len());
    for c in term.chars() {
        match c {
            '"' | '(' | ')' | '*' | ':' | '^' => {
                result.push('"');
                result.push(c);
                result.push('"');
            }
            _ => result.push(c),
        }
    }
    result
}

/// Escape a phrase for FTS5 query (inside double quotes)
fn escape_fts5_phrase(phrase: &str) -> String {
    // Inside quotes, only need to escape double quotes
    phrase.replace('"', "\"\"")
}

/// Parse a RediSearch query and return a ParsedQuery
pub fn parse_query(query: &str, verbatim: bool) -> Result<ParsedQuery, String> {
    let mut parser = QueryParser::new(query, verbatim);
    parser.parse()
}

/// Parse a query and return a nested array representation for FT.EXPLAIN
pub fn explain_query(query: &str, verbatim: bool) -> Result<Vec<ExplainNode>, String> {
    let mut parser = QueryParser::new(query, verbatim);
    let expr = parser.parse_expr()?;
    Ok(vec![expr_to_explain(&expr)])
}

/// A node in the explain tree
#[derive(Debug, Clone)]
pub enum ExplainNode {
    Text(String),
    Array(Vec<ExplainNode>),
}

impl ExplainNode {
    pub fn text(s: &str) -> Self {
        ExplainNode::Text(s.to_string())
    }

    pub fn array(nodes: Vec<ExplainNode>) -> Self {
        ExplainNode::Array(nodes)
    }
}

/// Convert a QueryExpr to an ExplainNode tree
fn expr_to_explain(expr: &QueryExpr) -> ExplainNode {
    match expr {
        QueryExpr::Term(t) => ExplainNode::array(vec![
            ExplainNode::text("TERM"),
            ExplainNode::text(t),
        ]),
        QueryExpr::Prefix(p) => ExplainNode::array(vec![
            ExplainNode::text("PREFIX"),
            ExplainNode::text(&format!("{}*", p)),
        ]),
        QueryExpr::Phrase(p) => ExplainNode::array(vec![
            ExplainNode::text("PHRASE"),
            ExplainNode::text(&format!("\"{}\"", p)),
        ]),
        QueryExpr::FieldText { field, expr } => ExplainNode::array(vec![
            ExplainNode::text("FIELD"),
            ExplainNode::text(field),
            expr_to_explain(expr),
        ]),
        QueryExpr::NumericRange { field, min, max } => {
            let min_str = match min {
                NumericBound::Inclusive(v) => format!("[{}", v),
                NumericBound::Exclusive(v) => format!("({}", v),
                NumericBound::Unbounded => "[-inf".to_string(),
            };
            let max_str = match max {
                NumericBound::Inclusive(v) => format!("{}]", v),
                NumericBound::Exclusive(v) => format!("{})", v),
                NumericBound::Unbounded => "+inf]".to_string(),
            };
            ExplainNode::array(vec![
                ExplainNode::text("NUMERIC"),
                ExplainNode::text(field),
                ExplainNode::text(&format!("{} {}", min_str, max_str)),
            ])
        }
        QueryExpr::TagMatch { field, tags } => ExplainNode::array(vec![
            ExplainNode::text("TAG"),
            ExplainNode::text(field),
            ExplainNode::text(&format!("{{{}}}", tags.join("|"))),
        ]),
        QueryExpr::And(exprs) => {
            let mut nodes = vec![ExplainNode::text("INTERSECT")];
            for e in exprs {
                nodes.push(expr_to_explain(e));
            }
            ExplainNode::array(nodes)
        }
        QueryExpr::Or(exprs) => {
            let mut nodes = vec![ExplainNode::text("UNION")];
            for e in exprs {
                nodes.push(expr_to_explain(e));
            }
            ExplainNode::array(nodes)
        }
        QueryExpr::Not(inner) => ExplainNode::array(vec![
            ExplainNode::text("NOT"),
            expr_to_explain(inner),
        ]),
        QueryExpr::MatchAll => ExplainNode::array(vec![
            ExplainNode::text("WILDCARD"),
            ExplainNode::text("*"),
        ]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_term() {
        let result = parse_query("hello", false).unwrap();
        assert_eq!(result.fts_query, Some("hello".to_string()));
    }

    #[test]
    fn test_multiple_terms() {
        let result = parse_query("hello world", false).unwrap();
        assert_eq!(result.fts_query, Some("hello AND world".to_string()));
    }

    #[test]
    fn test_or_operator() {
        let result = parse_query("hello | world", false).unwrap();
        assert_eq!(result.fts_query, Some("(hello OR world)".to_string()));
    }

    #[test]
    fn test_not_operator() {
        let result = parse_query("-hello", false).unwrap();
        assert_eq!(result.fts_query, Some("NOT hello".to_string()));
    }

    #[test]
    fn test_phrase() {
        let result = parse_query("\"hello world\"", false).unwrap();
        assert_eq!(result.fts_query, Some("\"hello world\"".to_string()));
    }

    #[test]
    fn test_prefix() {
        let result = parse_query("hel*", false).unwrap();
        assert_eq!(result.fts_query, Some("hel*".to_string()));
    }

    #[test]
    fn test_field_scoped_term() {
        let result = parse_query("@title:hello", false).unwrap();
        assert_eq!(result.fts_query, Some("\"title\":hello".to_string()));
        assert_eq!(result.search_fields, vec!["title"]);
    }

    #[test]
    fn test_field_scoped_phrase() {
        let result = parse_query("@title:\"hello world\"", false).unwrap();
        assert_eq!(
            result.fts_query,
            Some("\"title\":\"hello world\"".to_string())
        );
    }

    #[test]
    fn test_numeric_range() {
        let result = parse_query("@price:[10 100]", false).unwrap();
        assert!(result.fts_query.is_none());
        assert_eq!(result.numeric_filters.len(), 1);
        let filter = &result.numeric_filters[0];
        assert_eq!(filter.field, "price");
        assert_eq!(filter.min, NumericBound::Inclusive(10.0));
        assert_eq!(filter.max, NumericBound::Inclusive(100.0));
    }

    #[test]
    fn test_numeric_range_exclusive() {
        let result = parse_query("@price:[(10 (100]", false).unwrap();
        assert_eq!(result.numeric_filters.len(), 1);
        let filter = &result.numeric_filters[0];
        assert_eq!(filter.min, NumericBound::Exclusive(10.0));
        assert_eq!(filter.max, NumericBound::Exclusive(100.0));
    }

    #[test]
    fn test_tag_match() {
        let result = parse_query("@category:{electronics|books}", false).unwrap();
        assert!(result.fts_query.is_none());
        assert_eq!(result.tag_filters.len(), 1);
        let filter = &result.tag_filters[0];
        assert_eq!(filter.field, "category");
        assert_eq!(filter.tags, vec!["electronics", "books"]);
    }

    #[test]
    fn test_complex_query() {
        let result = parse_query("@title:hello @price:[10 100] -expensive", false).unwrap();
        assert!(result.fts_query.is_some());
        assert_eq!(result.numeric_filters.len(), 1);
    }

    #[test]
    fn test_match_all() {
        let result = parse_query("*", false).unwrap();
        assert!(result.fts_query.is_none());
    }

    #[test]
    fn test_grouped_or() {
        let result = parse_query("(hello | world) test", false).unwrap();
        assert!(result.fts_query.is_some());
    }
}
