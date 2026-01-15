#![no_main]

use libfuzzer_sys::fuzz_target;
use redlite::search::{parse_query, explain_query, parse_apply_expr, parse_filter_expr};

fuzz_target!(|data: &[u8]| {
    // Only fuzz valid UTF-8 strings
    if let Ok(query) = std::str::from_utf8(data) {
        // Limit query length to prevent excessive memory usage
        if query.len() > 10_000 {
            return;
        }

        // Test RediSearch query parser (verbatim = false)
        let _ = parse_query(query, false);

        // Test RediSearch query parser (verbatim = true)
        let _ = parse_query(query, true);

        // Test explain functionality
        let _ = explain_query(query, false);

        // Test APPLY expression parser
        let _ = parse_apply_expr(query);

        // Test FILTER expression parser
        let _ = parse_filter_expr(query);
    }
});
