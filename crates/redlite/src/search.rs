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
//! - Fuzzy/substring search: `%%term%%` (requires trigram tokenizer)
//! - Field-scoped text: `@title:hello`
//! - Field-scoped phrase: `@title:"hello world"`
//! - Field-scoped fuzzy: `@title:%%term%%`
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
//! - `%%term%%` -> `term` (trigram tokenizer enables substring matching)
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
    /// Fuzzy/substring search (%%term%%) - requires trigram tokenizer
    /// With Levenshtein: post-filters results by edit distance
    Fuzzy(String),
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
                    // FTS5 uses "A NOT B" syntax (NOT is a binary operator)
                    // Only append NOT if there's already a positive term
                    if result.fts_query.is_some() {
                        self.append_fts_query(result, &fts, "NOT");
                    }
                    // For standalone NOT (no preceding term), FTS5 can't handle it
                    // The in-memory fallback will handle this case
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
            // Fuzzy search: For trigram tokenizers, this enables substring matching
            // The term is quoted to preserve as a unit for matching
            QueryExpr::Fuzzy(t) => {
                let escaped = escape_fts5_phrase(t);
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

        // Check for fuzzy/substring search (%%term%%)
        if term.starts_with("%%") && term.ends_with("%%") && term.len() > 4 {
            let inner = term[2..term.len() - 2].to_string();
            if !inner.is_empty() {
                return Ok(QueryExpr::Fuzzy(inner));
            }
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
        QueryExpr::Fuzzy(f) => ExplainNode::array(vec![
            ExplainNode::text("FUZZY"),
            ExplainNode::text(&format!("%%{}%%", f)),
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

// ============================================================================
// APPLY and FILTER Expression Evaluation for FT.AGGREGATE
// ============================================================================

/// Value type for expression evaluation
#[derive(Debug, Clone, PartialEq)]
pub enum ExprValue {
    Number(f64),
    String(String),
    Bool(bool),
    Null,
}

impl ExprValue {
    pub fn as_number(&self) -> Option<f64> {
        match self {
            ExprValue::Number(n) => Some(*n),
            ExprValue::String(s) => s.parse().ok(),
            _ => None,
        }
    }

    pub fn as_string(&self) -> String {
        match self {
            ExprValue::Number(n) => n.to_string(),
            ExprValue::String(s) => s.clone(),
            ExprValue::Bool(b) => if *b { "1" } else { "0" }.to_string(),
            ExprValue::Null => String::new(),
        }
    }

    pub fn as_bool(&self) -> bool {
        match self {
            ExprValue::Bool(b) => *b,
            ExprValue::Number(n) => *n != 0.0,
            ExprValue::String(s) => !s.is_empty() && s != "0",
            ExprValue::Null => false,
        }
    }
}

/// APPLY expression AST
#[derive(Debug, Clone, PartialEq)]
pub enum ApplyExpr {
    /// Field reference: @fieldname
    Field(String),
    /// Literal number: 1.5
    Number(f64),
    /// Literal string: "text"
    LiteralString(String),
    /// Binary arithmetic: @a + @b, @price * 1.1
    BinaryOp {
        left: Box<ApplyExpr>,
        op: BinaryOp,
        right: Box<ApplyExpr>,
    },
    /// Unary minus: -@field
    Negate(Box<ApplyExpr>),
    /// String function: upper(@field), lower(@field)
    StringFunc {
        func: StringFunc,
        arg: Box<ApplyExpr>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StringFunc {
    Upper,
    Lower,
}

/// FILTER expression AST
#[derive(Debug, Clone, PartialEq)]
pub enum FilterExpr {
    /// Comparison: @field > 5, @count == 10
    Comparison {
        left: ApplyExpr,
        op: CompareOp,
        right: ApplyExpr,
    },
    /// Logical AND: expr1 AND expr2
    And(Box<FilterExpr>, Box<FilterExpr>),
    /// Logical OR: expr1 OR expr2
    Or(Box<FilterExpr>, Box<FilterExpr>),
    /// Logical NOT: NOT expr
    Not(Box<FilterExpr>),
    /// Parenthesized expression
    Paren(Box<FilterExpr>),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CompareOp {
    Eq,      // == or =
    Ne,      // != or <>
    Lt,      // <
    Le,      // <=
    Gt,      // >
    Ge,      // >=
}

/// Parse an APPLY expression string into an AST
pub fn parse_apply_expr(input: &str) -> Result<ApplyExpr, String> {
    let input = input.trim();
    if input.is_empty() {
        return Err("Empty expression".to_string());
    }
    parse_apply_additive(input)
}

fn parse_apply_additive(input: &str) -> Result<ApplyExpr, String> {
    let input = input.trim();

    // Find + or - at the top level (not inside parentheses or strings)
    let mut depth = 0;
    let mut in_string = false;
    let chars: Vec<char> = input.chars().collect();

    // Scan from right to left to get left-associativity
    for i in (0..chars.len()).rev() {
        let c = chars[i];
        match c {
            '"' if i == 0 || chars[i-1] != '\\' => in_string = !in_string,
            '(' if !in_string => depth += 1,
            ')' if !in_string => depth -= 1,
            '+' | '-' if !in_string && depth == 0 && i > 0 => {
                // Check it's not part of a number (e.g., 1.5e-10)
                let prev = chars[i-1];
                if prev == 'e' || prev == 'E' {
                    continue;
                }
                let left = &input[..i];
                let right = &input[i+1..];
                let left_expr = parse_apply_additive(left)?;
                let right_expr = parse_apply_multiplicative(right)?;
                let op = if c == '+' { BinaryOp::Add } else { BinaryOp::Sub };
                return Ok(ApplyExpr::BinaryOp {
                    left: Box::new(left_expr),
                    op,
                    right: Box::new(right_expr),
                });
            }
            _ => {}
        }
    }

    parse_apply_multiplicative(input)
}

fn parse_apply_multiplicative(input: &str) -> Result<ApplyExpr, String> {
    let input = input.trim();

    let mut depth = 0;
    let mut in_string = false;
    let chars: Vec<char> = input.chars().collect();

    for i in (0..chars.len()).rev() {
        let c = chars[i];
        match c {
            '"' if i == 0 || chars[i-1] != '\\' => in_string = !in_string,
            '(' if !in_string => depth += 1,
            ')' if !in_string => depth -= 1,
            '*' | '/' | '%' if !in_string && depth == 0 => {
                let left = &input[..i];
                let right = &input[i+1..];
                if left.trim().is_empty() {
                    continue;
                }
                let left_expr = parse_apply_multiplicative(left)?;
                let right_expr = parse_apply_unary(right)?;
                let op = match c {
                    '*' => BinaryOp::Mul,
                    '/' => BinaryOp::Div,
                    '%' => BinaryOp::Mod,
                    _ => unreachable!(),
                };
                return Ok(ApplyExpr::BinaryOp {
                    left: Box::new(left_expr),
                    op,
                    right: Box::new(right_expr),
                });
            }
            _ => {}
        }
    }

    parse_apply_unary(input)
}

fn parse_apply_unary(input: &str) -> Result<ApplyExpr, String> {
    let input = input.trim();

    if input.starts_with('-') && input.len() > 1 {
        let rest = &input[1..];
        // Make sure it's not a negative number
        if !rest.trim().chars().next().map(|c| c.is_ascii_digit()).unwrap_or(false) {
            let inner = parse_apply_unary(rest)?;
            return Ok(ApplyExpr::Negate(Box::new(inner)));
        }
    }

    parse_apply_primary(input)
}

fn parse_apply_primary(input: &str) -> Result<ApplyExpr, String> {
    let input = input.trim();

    // Parenthesized expression
    if input.starts_with('(') && input.ends_with(')') {
        return parse_apply_additive(&input[1..input.len()-1]);
    }

    // String function: upper(...) or lower(...)
    let input_lower = input.to_lowercase();
    if input_lower.starts_with("upper(") && input.ends_with(')') {
        let arg = &input[6..input.len()-1];
        let arg_expr = parse_apply_expr(arg)?;
        return Ok(ApplyExpr::StringFunc {
            func: StringFunc::Upper,
            arg: Box::new(arg_expr),
        });
    }
    if input_lower.starts_with("lower(") && input.ends_with(')') {
        let arg = &input[6..input.len()-1];
        let arg_expr = parse_apply_expr(arg)?;
        return Ok(ApplyExpr::StringFunc {
            func: StringFunc::Lower,
            arg: Box::new(arg_expr),
        });
    }

    // Field reference: @fieldname
    if input.starts_with('@') {
        let field = input[1..].to_string();
        if field.is_empty() {
            return Err("Empty field reference".to_string());
        }
        return Ok(ApplyExpr::Field(field));
    }

    // String literal: "text"
    if input.starts_with('"') && input.ends_with('"') && input.len() >= 2 {
        let s = input[1..input.len()-1].replace("\\\"", "\"");
        return Ok(ApplyExpr::LiteralString(s));
    }

    // Number
    if let Ok(n) = input.parse::<f64>() {
        return Ok(ApplyExpr::Number(n));
    }

    // Bare identifier - treat as string literal
    Ok(ApplyExpr::LiteralString(input.to_string()))
}

/// Evaluate an APPLY expression against a row
pub fn evaluate_apply_expr(
    expr: &ApplyExpr,
    row: &std::collections::HashMap<String, String>,
) -> ExprValue {
    match expr {
        ApplyExpr::Field(name) => {
            match row.get(name) {
                Some(v) => {
                    if let Ok(n) = v.parse::<f64>() {
                        ExprValue::Number(n)
                    } else {
                        ExprValue::String(v.clone())
                    }
                }
                None => ExprValue::Null,
            }
        }
        ApplyExpr::Number(n) => ExprValue::Number(*n),
        ApplyExpr::LiteralString(s) => ExprValue::String(s.clone()),
        ApplyExpr::BinaryOp { left, op, right } => {
            let left_val = evaluate_apply_expr(left, row);
            let right_val = evaluate_apply_expr(right, row);

            // Try numeric operation first
            if let (Some(l), Some(r)) = (left_val.as_number(), right_val.as_number()) {
                let result = match op {
                    BinaryOp::Add => l + r,
                    BinaryOp::Sub => l - r,
                    BinaryOp::Mul => l * r,
                    BinaryOp::Div => if r != 0.0 { l / r } else { f64::NAN },
                    BinaryOp::Mod => if r != 0.0 { l % r } else { f64::NAN },
                };
                ExprValue::Number(result)
            } else if *op == BinaryOp::Add {
                // String concatenation for + with non-numbers
                ExprValue::String(format!("{}{}", left_val.as_string(), right_val.as_string()))
            } else {
                ExprValue::Null
            }
        }
        ApplyExpr::Negate(inner) => {
            let val = evaluate_apply_expr(inner, row);
            if let Some(n) = val.as_number() {
                ExprValue::Number(-n)
            } else {
                ExprValue::Null
            }
        }
        ApplyExpr::StringFunc { func, arg } => {
            let val = evaluate_apply_expr(arg, row);
            let s = val.as_string();
            match func {
                StringFunc::Upper => ExprValue::String(s.to_uppercase()),
                StringFunc::Lower => ExprValue::String(s.to_lowercase()),
            }
        }
    }
}

/// Parse a FILTER expression string into an AST
pub fn parse_filter_expr(input: &str) -> Result<FilterExpr, String> {
    let input = input.trim();
    if input.is_empty() {
        return Err("Empty filter expression".to_string());
    }
    parse_filter_or(input)
}

fn parse_filter_or(input: &str) -> Result<FilterExpr, String> {
    let input = input.trim();

    // Find OR at the top level
    let mut depth = 0;
    let mut in_string = false;
    let chars: Vec<char> = input.chars().collect();

    for i in 0..chars.len().saturating_sub(1) {
        let c = chars[i];
        match c {
            '"' if i == 0 || chars[i-1] != '\\' => in_string = !in_string,
            '(' if !in_string => depth += 1,
            ')' if !in_string => depth -= 1,
            'O' | 'o' if !in_string && depth == 0 => {
                if i + 2 < chars.len() {
                    let next = chars[i + 1];
                    if (next == 'R' || next == 'r') &&
                       (i + 2 >= chars.len() || !chars[i + 2].is_alphanumeric()) &&
                       (i == 0 || !chars[i - 1].is_alphanumeric()) {
                        let left = input[..i].trim();
                        let right = input[i + 2..].trim();
                        if !left.is_empty() && !right.is_empty() {
                            let left_expr = parse_filter_or(left)?;
                            let right_expr = parse_filter_and(right)?;
                            return Ok(FilterExpr::Or(Box::new(left_expr), Box::new(right_expr)));
                        }
                    }
                }
            }
            '|' if !in_string && depth == 0 => {
                if i + 1 < chars.len() && chars[i + 1] == '|' {
                    let left = input[..i].trim();
                    let right = input[i + 2..].trim();
                    if !left.is_empty() && !right.is_empty() {
                        let left_expr = parse_filter_or(left)?;
                        let right_expr = parse_filter_and(right)?;
                        return Ok(FilterExpr::Or(Box::new(left_expr), Box::new(right_expr)));
                    }
                }
            }
            _ => {}
        }
    }

    parse_filter_and(input)
}

fn parse_filter_and(input: &str) -> Result<FilterExpr, String> {
    let input = input.trim();

    let mut depth = 0;
    let mut in_string = false;
    let chars: Vec<char> = input.chars().collect();

    for i in 0..chars.len().saturating_sub(2) {
        let c = chars[i];
        match c {
            '"' if i == 0 || chars[i-1] != '\\' => in_string = !in_string,
            '(' if !in_string => depth += 1,
            ')' if !in_string => depth -= 1,
            'A' | 'a' if !in_string && depth == 0 => {
                if i + 3 <= chars.len() {
                    let next1 = chars[i + 1];
                    let next2 = chars[i + 2];
                    if (next1 == 'N' || next1 == 'n') && (next2 == 'D' || next2 == 'd') &&
                       (i + 3 >= chars.len() || !chars[i + 3].is_alphanumeric()) &&
                       (i == 0 || !chars[i - 1].is_alphanumeric()) {
                        let left = input[..i].trim();
                        let right = input[i + 3..].trim();
                        if !left.is_empty() && !right.is_empty() {
                            let left_expr = parse_filter_and(left)?;
                            let right_expr = parse_filter_not(right)?;
                            return Ok(FilterExpr::And(Box::new(left_expr), Box::new(right_expr)));
                        }
                    }
                }
            }
            '&' if !in_string && depth == 0 => {
                if i + 1 < chars.len() && chars[i + 1] == '&' {
                    let left = input[..i].trim();
                    let right = input[i + 2..].trim();
                    if !left.is_empty() && !right.is_empty() {
                        let left_expr = parse_filter_and(left)?;
                        let right_expr = parse_filter_not(right)?;
                        return Ok(FilterExpr::And(Box::new(left_expr), Box::new(right_expr)));
                    }
                }
            }
            _ => {}
        }
    }

    parse_filter_not(input)
}

fn parse_filter_not(input: &str) -> Result<FilterExpr, String> {
    let input = input.trim();

    let input_lower = input.to_lowercase();
    if input_lower.starts_with("not ") {
        let rest = &input[4..];
        let inner = parse_filter_not(rest)?;
        return Ok(FilterExpr::Not(Box::new(inner)));
    }
    if input.starts_with('!') && input.len() > 1 {
        let rest = &input[1..];
        let inner = parse_filter_not(rest)?;
        return Ok(FilterExpr::Not(Box::new(inner)));
    }

    parse_filter_primary(input)
}

fn parse_filter_primary(input: &str) -> Result<FilterExpr, String> {
    let input = input.trim();

    // Parenthesized expression
    if input.starts_with('(') && input.ends_with(')') {
        let inner = &input[1..input.len()-1];
        let expr = parse_filter_or(inner)?;
        return Ok(FilterExpr::Paren(Box::new(expr)));
    }

    // Comparison expression - find the operator
    let ops = [
        ("==", CompareOp::Eq),
        ("!=", CompareOp::Ne),
        ("<=", CompareOp::Le),
        (">=", CompareOp::Ge),
        ("<>", CompareOp::Ne),
        ("<", CompareOp::Lt),
        (">", CompareOp::Gt),
        ("=", CompareOp::Eq),
    ];

    for (op_str, op) in &ops {
        // Find operator not inside strings or parentheses
        let mut depth = 0;
        let mut in_string = false;
        let chars: Vec<char> = input.chars().collect();

        for i in 0..chars.len() {
            let c = chars[i];
            match c {
                '"' if i == 0 || chars[i-1] != '\\' => in_string = !in_string,
                '(' if !in_string => depth += 1,
                ')' if !in_string => depth -= 1,
                _ if !in_string && depth == 0 => {
                    if input[i..].starts_with(op_str) {
                        let left = &input[..i];
                        let right = &input[i + op_str.len()..];
                        if !left.trim().is_empty() && !right.trim().is_empty() {
                            let left_expr = parse_apply_expr(left)?;
                            let right_expr = parse_apply_expr(right)?;
                            return Ok(FilterExpr::Comparison {
                                left: left_expr,
                                op: *op,
                                right: right_expr,
                            });
                        }
                    }
                }
                _ => {}
            }
        }
    }

    Err(format!("Invalid filter expression: {}", input))
}

/// Evaluate a FILTER expression against a row
pub fn evaluate_filter_expr(
    expr: &FilterExpr,
    row: &std::collections::HashMap<String, String>,
) -> bool {
    match expr {
        FilterExpr::Comparison { left, op, right } => {
            let left_val = evaluate_apply_expr(left, row);
            let right_val = evaluate_apply_expr(right, row);

            // Try numeric comparison first
            if let (Some(l), Some(r)) = (left_val.as_number(), right_val.as_number()) {
                return match op {
                    CompareOp::Eq => (l - r).abs() < f64::EPSILON,
                    CompareOp::Ne => (l - r).abs() >= f64::EPSILON,
                    CompareOp::Lt => l < r,
                    CompareOp::Le => l <= r,
                    CompareOp::Gt => l > r,
                    CompareOp::Ge => l >= r,
                };
            }

            // Fall back to string comparison
            let l = left_val.as_string();
            let r = right_val.as_string();
            match op {
                CompareOp::Eq => l == r,
                CompareOp::Ne => l != r,
                CompareOp::Lt => l < r,
                CompareOp::Le => l <= r,
                CompareOp::Gt => l > r,
                CompareOp::Ge => l >= r,
            }
        }
        FilterExpr::And(left, right) => {
            evaluate_filter_expr(left, row) && evaluate_filter_expr(right, row)
        }
        FilterExpr::Or(left, right) => {
            evaluate_filter_expr(left, row) || evaluate_filter_expr(right, row)
        }
        FilterExpr::Not(inner) => {
            !evaluate_filter_expr(inner, row)
        }
        FilterExpr::Paren(inner) => {
            evaluate_filter_expr(inner, row)
        }
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
    fn test_not_operator_standalone() {
        // Standalone NOT can't be expressed in FTS5 (requires preceding term)
        // Falls back to in-memory matching
        let result = parse_query("-hello", false).unwrap();
        assert_eq!(result.fts_query, None);
    }

    #[test]
    fn test_not_operator_with_term() {
        // NOT with preceding term uses FTS5 binary NOT syntax
        let result = parse_query("world -hello", false).unwrap();
        assert_eq!(result.fts_query, Some("world NOT hello".to_string()));
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

    // ========================================================================
    // APPLY Expression Tests
    // ========================================================================

    #[test]
    fn test_apply_field_reference() {
        let expr = parse_apply_expr("@price").unwrap();
        assert_eq!(expr, ApplyExpr::Field("price".to_string()));
    }

    #[test]
    fn test_apply_number_literal() {
        let expr = parse_apply_expr("1.5").unwrap();
        assert_eq!(expr, ApplyExpr::Number(1.5));
    }

    #[test]
    fn test_apply_negative_number() {
        let expr = parse_apply_expr("-10").unwrap();
        assert_eq!(expr, ApplyExpr::Number(-10.0));
    }

    #[test]
    fn test_apply_arithmetic_multiply() {
        let expr = parse_apply_expr("@price * 1.1").unwrap();
        match expr {
            ApplyExpr::BinaryOp { left, op, right } => {
                assert_eq!(*left, ApplyExpr::Field("price".to_string()));
                assert_eq!(op, BinaryOp::Mul);
                assert_eq!(*right, ApplyExpr::Number(1.1));
            }
            _ => panic!("Expected BinaryOp"),
        }
    }

    #[test]
    fn test_apply_arithmetic_add_fields() {
        let expr = parse_apply_expr("@a + @b").unwrap();
        match expr {
            ApplyExpr::BinaryOp { left, op, right } => {
                assert_eq!(*left, ApplyExpr::Field("a".to_string()));
                assert_eq!(op, BinaryOp::Add);
                assert_eq!(*right, ApplyExpr::Field("b".to_string()));
            }
            _ => panic!("Expected BinaryOp"),
        }
    }

    #[test]
    fn test_apply_arithmetic_complex() {
        // @a + @b * 2 should parse as @a + (@b * 2) due to precedence
        let expr = parse_apply_expr("@a + @b * 2").unwrap();
        match expr {
            ApplyExpr::BinaryOp { left, op: BinaryOp::Add, right } => {
                assert_eq!(*left, ApplyExpr::Field("a".to_string()));
                match *right {
                    ApplyExpr::BinaryOp { left: inner_left, op: BinaryOp::Mul, right: inner_right } => {
                        assert_eq!(*inner_left, ApplyExpr::Field("b".to_string()));
                        assert_eq!(*inner_right, ApplyExpr::Number(2.0));
                    }
                    _ => panic!("Expected inner BinaryOp"),
                }
            }
            _ => panic!("Expected BinaryOp"),
        }
    }

    #[test]
    fn test_apply_upper_function() {
        let expr = parse_apply_expr("upper(@name)").unwrap();
        match expr {
            ApplyExpr::StringFunc { func: StringFunc::Upper, arg } => {
                assert_eq!(*arg, ApplyExpr::Field("name".to_string()));
            }
            _ => panic!("Expected StringFunc"),
        }
    }

    #[test]
    fn test_apply_lower_function() {
        let expr = parse_apply_expr("lower(@title)").unwrap();
        match expr {
            ApplyExpr::StringFunc { func: StringFunc::Lower, arg } => {
                assert_eq!(*arg, ApplyExpr::Field("title".to_string()));
            }
            _ => panic!("Expected StringFunc"),
        }
    }

    #[test]
    fn test_apply_parentheses() {
        let expr = parse_apply_expr("(@a + @b) * 2").unwrap();
        match expr {
            ApplyExpr::BinaryOp { left, op: BinaryOp::Mul, right } => {
                match *left {
                    ApplyExpr::BinaryOp { left: inner_left, op: BinaryOp::Add, right: inner_right } => {
                        assert_eq!(*inner_left, ApplyExpr::Field("a".to_string()));
                        assert_eq!(*inner_right, ApplyExpr::Field("b".to_string()));
                    }
                    _ => panic!("Expected inner BinaryOp"),
                }
                assert_eq!(*right, ApplyExpr::Number(2.0));
            }
            _ => panic!("Expected BinaryOp"),
        }
    }

    #[test]
    fn test_apply_evaluate_field() {
        let mut row = std::collections::HashMap::new();
        row.insert("price".to_string(), "100".to_string());

        let expr = parse_apply_expr("@price").unwrap();
        let result = evaluate_apply_expr(&expr, &row);
        assert_eq!(result.as_number(), Some(100.0));
    }

    #[test]
    fn test_apply_evaluate_multiply() {
        let mut row = std::collections::HashMap::new();
        row.insert("price".to_string(), "100".to_string());

        let expr = parse_apply_expr("@price * 1.1").unwrap();
        let result = evaluate_apply_expr(&expr, &row);
        assert!((result.as_number().unwrap() - 110.0).abs() < 0.001);
    }

    #[test]
    fn test_apply_evaluate_add_fields() {
        let mut row = std::collections::HashMap::new();
        row.insert("a".to_string(), "10".to_string());
        row.insert("b".to_string(), "20".to_string());

        let expr = parse_apply_expr("@a + @b").unwrap();
        let result = evaluate_apply_expr(&expr, &row);
        assert_eq!(result.as_number(), Some(30.0));
    }

    #[test]
    fn test_apply_evaluate_division() {
        let mut row = std::collections::HashMap::new();
        row.insert("total".to_string(), "100".to_string());
        row.insert("count".to_string(), "4".to_string());

        let expr = parse_apply_expr("@total / @count").unwrap();
        let result = evaluate_apply_expr(&expr, &row);
        assert_eq!(result.as_number(), Some(25.0));
    }

    #[test]
    fn test_apply_evaluate_upper() {
        let mut row = std::collections::HashMap::new();
        row.insert("name".to_string(), "hello".to_string());

        let expr = parse_apply_expr("upper(@name)").unwrap();
        let result = evaluate_apply_expr(&expr, &row);
        assert_eq!(result.as_string(), "HELLO");
    }

    #[test]
    fn test_apply_evaluate_lower() {
        let mut row = std::collections::HashMap::new();
        row.insert("name".to_string(), "HELLO".to_string());

        let expr = parse_apply_expr("lower(@name)").unwrap();
        let result = evaluate_apply_expr(&expr, &row);
        assert_eq!(result.as_string(), "hello");
    }

    #[test]
    fn test_apply_evaluate_null_field() {
        let row = std::collections::HashMap::new();

        let expr = parse_apply_expr("@missing").unwrap();
        let result = evaluate_apply_expr(&expr, &row);
        assert_eq!(result, ExprValue::Null);
    }

    #[test]
    fn test_apply_evaluate_string_concat() {
        let mut row = std::collections::HashMap::new();
        row.insert("first".to_string(), "Hello".to_string());
        row.insert("last".to_string(), "World".to_string());

        let expr = parse_apply_expr("@first + @last").unwrap();
        let result = evaluate_apply_expr(&expr, &row);
        assert_eq!(result.as_string(), "HelloWorld");
    }

    // ========================================================================
    // FILTER Expression Tests
    // ========================================================================

    #[test]
    fn test_filter_comparison_gt() {
        let expr = parse_filter_expr("@count > 5").unwrap();
        match expr {
            FilterExpr::Comparison { left, op, right } => {
                assert_eq!(left, ApplyExpr::Field("count".to_string()));
                assert_eq!(op, CompareOp::Gt);
                assert_eq!(right, ApplyExpr::Number(5.0));
            }
            _ => panic!("Expected Comparison"),
        }
    }

    #[test]
    fn test_filter_comparison_le() {
        let expr = parse_filter_expr("@price <= 100").unwrap();
        match expr {
            FilterExpr::Comparison { left, op, right } => {
                assert_eq!(left, ApplyExpr::Field("price".to_string()));
                assert_eq!(op, CompareOp::Le);
                assert_eq!(right, ApplyExpr::Number(100.0));
            }
            _ => panic!("Expected Comparison"),
        }
    }

    #[test]
    fn test_filter_comparison_eq() {
        let expr = parse_filter_expr("@status == \"active\"").unwrap();
        match expr {
            FilterExpr::Comparison { left, op, right } => {
                assert_eq!(left, ApplyExpr::Field("status".to_string()));
                assert_eq!(op, CompareOp::Eq);
                assert_eq!(right, ApplyExpr::LiteralString("active".to_string()));
            }
            _ => panic!("Expected Comparison"),
        }
    }

    #[test]
    fn test_filter_comparison_ne() {
        let expr = parse_filter_expr("@value != 0").unwrap();
        match expr {
            FilterExpr::Comparison { left, op, right } => {
                assert_eq!(left, ApplyExpr::Field("value".to_string()));
                assert_eq!(op, CompareOp::Ne);
                assert_eq!(right, ApplyExpr::Number(0.0));
            }
            _ => panic!("Expected Comparison"),
        }
    }

    #[test]
    fn test_filter_logical_and() {
        let expr = parse_filter_expr("@a > 5 AND @b < 10").unwrap();
        match expr {
            FilterExpr::And(left, right) => {
                match *left {
                    FilterExpr::Comparison { op: CompareOp::Gt, .. } => {}
                    _ => panic!("Expected Comparison for left"),
                }
                match *right {
                    FilterExpr::Comparison { op: CompareOp::Lt, .. } => {}
                    _ => panic!("Expected Comparison for right"),
                }
            }
            _ => panic!("Expected And"),
        }
    }

    #[test]
    fn test_filter_logical_or() {
        let expr = parse_filter_expr("@a > 5 OR @b < 10").unwrap();
        match expr {
            FilterExpr::Or(_, _) => {}
            _ => panic!("Expected Or"),
        }
    }

    #[test]
    fn test_filter_logical_not() {
        let expr = parse_filter_expr("NOT @active == 0").unwrap();
        match expr {
            FilterExpr::Not(_) => {}
            _ => panic!("Expected Not"),
        }
    }

    #[test]
    fn test_filter_parentheses() {
        let expr = parse_filter_expr("(@a > 5 OR @b > 5) AND @c < 10").unwrap();
        match expr {
            FilterExpr::And(left, _) => {
                match *left {
                    FilterExpr::Paren(inner) => {
                        match *inner {
                            FilterExpr::Or(_, _) => {}
                            _ => panic!("Expected Or inside Paren"),
                        }
                    }
                    _ => panic!("Expected Paren"),
                }
            }
            _ => panic!("Expected And"),
        }
    }

    #[test]
    fn test_filter_evaluate_gt_true() {
        let mut row = std::collections::HashMap::new();
        row.insert("count".to_string(), "10".to_string());

        let expr = parse_filter_expr("@count > 5").unwrap();
        assert!(evaluate_filter_expr(&expr, &row));
    }

    #[test]
    fn test_filter_evaluate_gt_false() {
        let mut row = std::collections::HashMap::new();
        row.insert("count".to_string(), "3".to_string());

        let expr = parse_filter_expr("@count > 5").unwrap();
        assert!(!evaluate_filter_expr(&expr, &row));
    }

    #[test]
    fn test_filter_evaluate_eq_string() {
        let mut row = std::collections::HashMap::new();
        row.insert("status".to_string(), "active".to_string());

        let expr = parse_filter_expr("@status == \"active\"").unwrap();
        assert!(evaluate_filter_expr(&expr, &row));
    }

    #[test]
    fn test_filter_evaluate_and() {
        let mut row = std::collections::HashMap::new();
        row.insert("a".to_string(), "10".to_string());
        row.insert("b".to_string(), "5".to_string());

        let expr = parse_filter_expr("@a > 5 AND @b < 10").unwrap();
        assert!(evaluate_filter_expr(&expr, &row));
    }

    #[test]
    fn test_filter_evaluate_or() {
        let mut row = std::collections::HashMap::new();
        row.insert("a".to_string(), "3".to_string());
        row.insert("b".to_string(), "5".to_string());

        // a > 5 is false, b < 10 is true, so OR should be true
        let expr = parse_filter_expr("@a > 5 OR @b < 10").unwrap();
        assert!(evaluate_filter_expr(&expr, &row));
    }

    #[test]
    fn test_filter_evaluate_not() {
        let mut row = std::collections::HashMap::new();
        row.insert("active".to_string(), "1".to_string());

        // active == 0 is false, NOT false is true
        let expr = parse_filter_expr("NOT @active == 0").unwrap();
        assert!(evaluate_filter_expr(&expr, &row));
    }

    #[test]
    fn test_filter_evaluate_complex() {
        let mut row = std::collections::HashMap::new();
        row.insert("price".to_string(), "50".to_string());
        row.insert("count".to_string(), "10".to_string());
        row.insert("status".to_string(), "active".to_string());

        // (price > 20 AND count >= 10) OR status != "active"
        // (true AND true) OR false = true
        let expr = parse_filter_expr("(@price > 20 AND @count >= 10) OR @status != \"active\"").unwrap();
        assert!(evaluate_filter_expr(&expr, &row));
    }

    #[test]
    fn test_filter_double_ampersand() {
        let expr = parse_filter_expr("@a > 5 && @b < 10").unwrap();
        match expr {
            FilterExpr::And(_, _) => {}
            _ => panic!("Expected And from &&"),
        }
    }

    #[test]
    fn test_filter_double_pipe() {
        let expr = parse_filter_expr("@a > 5 || @b < 10").unwrap();
        match expr {
            FilterExpr::Or(_, _) => {}
            _ => panic!("Expected Or from ||"),
        }
    }

    // ========================================================================
    // Phase 1: Query Parser Edge Cases
    // ========================================================================

    #[test]
    fn test_query_empty() {
        let result = parse_query("", false).unwrap();
        assert!(result.fts_query.is_none());
    }

    #[test]
    fn test_query_whitespace_only() {
        let result = parse_query("   ", false).unwrap();
        assert!(result.fts_query.is_none());
    }

    #[test]
    fn test_query_single_term() {
        let result = parse_query("test", false).unwrap();
        assert_eq!(result.fts_query, Some("test".to_string()));
    }

    #[test]
    fn test_query_unicode_japanese() {
        let result = parse_query("日本語", false).unwrap();
        assert_eq!(result.fts_query, Some("日本語".to_string()));
    }

    #[test]
    fn test_query_unicode_arabic() {
        let result = parse_query("مرحبا", false).unwrap();
        assert_eq!(result.fts_query, Some("مرحبا".to_string()));
    }

    #[test]
    fn test_query_unicode_emoji() {
        let result = parse_query("🎉", false).unwrap();
        assert_eq!(result.fts_query, Some("🎉".to_string()));
    }

    #[test]
    fn test_query_unicode_mixed() {
        let result = parse_query("hello 世界 مرحبا", false).unwrap();
        assert!(result.fts_query.is_some());
        let query = result.fts_query.unwrap();
        assert!(query.contains("hello"));
        assert!(query.contains("世界"));
        assert!(query.contains("مرحبا"));
    }

    #[test]
    fn test_query_very_long() {
        // Generate a query with 100 terms
        let terms: Vec<&str> = (0..100).map(|_| "term").collect();
        let long_query = terms.join(" ");
        let result = parse_query(&long_query, false).unwrap();
        assert!(result.fts_query.is_some());
    }

    #[test]
    fn test_query_nested_parentheses() {
        let result = parse_query("((a | b) c) | d", false).unwrap();
        assert!(result.fts_query.is_some());
    }

    #[test]
    fn test_query_deeply_nested_parentheses() {
        let result = parse_query("(((a | b))) c", false).unwrap();
        assert!(result.fts_query.is_some());
    }

    #[test]
    fn test_query_operator_precedence_or_and() {
        // a | b c should be parsed as (a) OR (b AND c)
        let result = parse_query("a | b c", false).unwrap();
        assert!(result.fts_query.is_some());
        let query = result.fts_query.unwrap();
        // OR has lower precedence, so b c should be grouped
        assert!(query.contains("OR"));
        assert!(query.contains("AND"));
    }

    #[test]
    fn test_query_phrase_with_special_chars() {
        let result = parse_query("\"hello, world!\"", false).unwrap();
        assert!(result.fts_query.is_some());
        assert!(result.fts_query.unwrap().contains("hello, world!"));
    }

    #[test]
    fn test_query_phrase_with_apostrophe() {
        let result = parse_query("\"test's value\"", false).unwrap();
        assert!(result.fts_query.is_some());
    }

    #[test]
    fn test_query_prefix_short_stem() {
        let result = parse_query("a*", false).unwrap();
        assert!(result.fts_query.is_some());
        assert!(result.fts_query.unwrap().contains("a*"));
    }

    #[test]
    fn test_query_prefix_two_chars() {
        let result = parse_query("ab*", false).unwrap();
        assert!(result.fts_query.is_some());
        assert!(result.fts_query.unwrap().contains("ab*"));
    }

    #[test]
    fn test_query_field_scoped_with_or() {
        let result = parse_query("@title:(a | b)", false).unwrap();
        assert!(result.fts_query.is_some());
        assert!(result.search_fields.contains(&"title".to_string()));
    }

    #[test]
    fn test_query_field_scoped_with_not() {
        let result = parse_query("@title:hello -@body:world", false).unwrap();
        assert!(result.fts_query.is_some());
    }

    #[test]
    fn test_query_numeric_range_zero() {
        let result = parse_query("@value:[0 0]", false).unwrap();
        assert_eq!(result.numeric_filters.len(), 1);
        let filter = &result.numeric_filters[0];
        assert_eq!(filter.min, NumericBound::Inclusive(0.0));
        assert_eq!(filter.max, NumericBound::Inclusive(0.0));
    }

    #[test]
    fn test_query_numeric_range_infinity() {
        let result = parse_query("@value:[-inf +inf]", false).unwrap();
        assert_eq!(result.numeric_filters.len(), 1);
        let filter = &result.numeric_filters[0];
        assert_eq!(filter.min, NumericBound::Unbounded);
        assert_eq!(filter.max, NumericBound::Unbounded);
    }

    #[test]
    fn test_query_numeric_range_negative() {
        let result = parse_query("@temp:[-50 50]", false).unwrap();
        assert_eq!(result.numeric_filters.len(), 1);
        let filter = &result.numeric_filters[0];
        assert_eq!(filter.min, NumericBound::Inclusive(-50.0));
        assert_eq!(filter.max, NumericBound::Inclusive(50.0));
    }

    #[test]
    fn test_query_tag_empty() {
        let result = parse_query("@category:{}", false).unwrap();
        assert_eq!(result.tag_filters.len(), 1);
        // Empty tag filter should have no tags or empty
    }

    #[test]
    fn test_query_tag_with_spaces_quoted() {
        // Tags with spaces must be quoted
        let result = parse_query("@category:{\"science fiction\"|fantasy}", false).unwrap();
        assert_eq!(result.tag_filters.len(), 1);
        let filter = &result.tag_filters[0];
        assert!(filter.tags.contains(&"science fiction".to_string()));
        assert!(filter.tags.contains(&"fantasy".to_string()));
    }

    #[test]
    fn test_query_tag_unquoted_simple() {
        let result = parse_query("@category:{electronics|books}", false).unwrap();
        assert_eq!(result.tag_filters.len(), 1);
        let filter = &result.tag_filters[0];
        assert!(filter.tags.contains(&"electronics".to_string()));
        assert!(filter.tags.contains(&"books".to_string()));
    }

    #[test]
    fn test_query_mixed_all_types() {
        let result = parse_query("@title:hello @price:[10 100] @category:{books}", false).unwrap();
        assert!(result.fts_query.is_some());
        assert_eq!(result.numeric_filters.len(), 1);
        assert_eq!(result.tag_filters.len(), 1);
    }

    #[test]
    fn test_query_multiple_or() {
        let result = parse_query("a | b | c | d", false).unwrap();
        assert!(result.fts_query.is_some());
    }

    #[test]
    fn test_query_multiple_not() {
        let result = parse_query("hello -world -foo", false).unwrap();
        assert!(result.fts_query.is_some());
    }

    #[test]
    fn test_query_mixed_operators() {
        let result = parse_query("(a | b) -c d", false).unwrap();
        assert!(result.fts_query.is_some());
    }

    // ========================================================================
    // BM25 Scoring Related Tests (parser side)
    // ========================================================================

    #[test]
    fn test_query_multiple_same_term() {
        // Multiple occurrences of same term
        let result = parse_query("test test test", false).unwrap();
        assert!(result.fts_query.is_some());
    }

    #[test]
    fn test_query_phrase_vs_terms() {
        // Phrase match should be different from individual terms
        let phrase_result = parse_query("\"hello world\"", false).unwrap();
        let terms_result = parse_query("hello world", false).unwrap();

        // Phrase should have quotes in the FTS5 query
        assert!(phrase_result.fts_query.unwrap().contains("\"hello world\""));
        // Terms should have AND
        assert!(terms_result.fts_query.unwrap().contains("AND"));
    }
}
