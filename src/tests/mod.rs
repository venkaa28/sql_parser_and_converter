#[cfg(test)]
mod tests {
    use crate::parser;
    use serde::{Deserialize, Serialize};
    use serde_json::Value;
    use std::fs;
    use std::path::Path;

    #[derive(Debug, Serialize, Deserialize)]
    struct TestCase {
        query: String,
        expected_ast: Value,
        expected_substrait: String,
    }

    #[cfg(test)]
    fn load_test_cases() -> Vec<TestCase> {
        let test_cases_path = Path::new("src/queries.json"); // Path is now relative to the cargo manifest directory
        let contents = fs::read_to_string(test_cases_path).expect("file not found");

        serde_json::from_str(&contents).expect("JSON was not well-formatted")
    }

    #[test]
    fn test_string_to_ast_query_1() {
        let test_cases = load_test_cases();

        let test_case = &test_cases[0];

        match parser::parse_handler(&test_case.query) {
            Ok((remaining, ast)) => {
                let ast_json = serde_json::to_value(ast).expect("Failed to serialize AST");
                assert_eq!(
                    ast_json, test_case.expected_ast,
                    "AST does not match for query: {}",
                    test_case.query
                );
                println!(
                    "Query: {}\nParsed AST: {:?}, Remaining: '{}'",
                    test_case.query, ast_json, remaining
                );
            }
            Err(e) => panic!("Failed to parse query: {}\n  {:?}", test_case.query, e),
        }
    }

    #[test]
    fn test_string_to_ast_query_2() {
        let test_cases = load_test_cases();

        let test_case = &test_cases[1];

        match parser::parse_handler(&test_case.query) {
            Ok((remaining, ast)) => {
                let ast_json = serde_json::to_value(ast).expect("Failed to serialize AST");
                assert_eq!(
                    ast_json, test_case.expected_ast,
                    "AST does not match for query: {}",
                    test_case.query
                );
                println!(
                    "Query: {}\nParsed AST: {:?}, Remaining: '{}'",
                    test_case.query, ast_json, remaining
                );
            }
            Err(e) => panic!("Failed to parse query: {}\n  {:?}", test_case.query, e),
        }
    }

    #[test]
    fn test_string_to_ast_query_3() {
        let test_cases = load_test_cases();

        let test_case = &test_cases[2];

        match parser::parse_handler(&test_case.query) {
            Ok((remaining, ast)) => {
                let ast_json = serde_json::to_value(ast).expect("Failed to serialize AST");
                assert_eq!(
                    ast_json, test_case.expected_ast,
                    "AST does not match for query: {}",
                    test_case.query
                );
                println!(
                    "Query: {}\nParsed AST: {:?}, Remaining: '{}'",
                    test_case.query, ast_json, remaining
                );
            }
            Err(e) => panic!("Failed to parse query: {}\n  {:?}", test_case.query, e),
        }
    }

    #[test]
    fn test_string_to_ast_query_4() {
        let test_cases = load_test_cases();

        let test_case = &test_cases[3];

        match parser::parse_handler(&test_case.query) {
            Ok((remaining, ast)) => {
                let ast_json = serde_json::to_value(ast).expect("Failed to serialize AST");
                assert_eq!(
                    ast_json, test_case.expected_ast,
                    "AST does not match for query: {}",
                    test_case.query
                );
                println!(
                    "Query: {}\nParsed AST: {:?}, Remaining: '{}'",
                    test_case.query, ast_json, remaining
                );
            }
            Err(e) => panic!("Failed to parse query: {}\n  {:?}", test_case.query, e),
        }
    }

    #[test]
    fn test_string_to_ast_query_5() {
        let test_cases = load_test_cases();

        let test_case = &test_cases[4];

        match parser::parse_handler(&test_case.query) {
            Ok((remaining, ast)) => {
                let ast_json = serde_json::to_value(ast).expect("Failed to serialize AST");
                assert_eq!(
                    ast_json, test_case.expected_ast,
                    "AST does not match for query: {}",
                    test_case.query
                );
                println!(
                    "Query: {}\nParsed AST: {:?}, Remaining: '{}'",
                    test_case.query, ast_json, remaining
                );
            }
            Err(e) => panic!("Failed to parse query: {}\n  {:?}", test_case.query, e),
        }
    }

    #[test]
    fn test_string_to_ast_failure() {
        let fail_query = "SELECT SELECT COUNT(*) FROM hits;";

        let result = parser::parse_handler(&fail_query);
        assert!(
            matches!(result, Err(_)),
            "Query should have failed to parse, but it succeeded."
        );
    }
}
