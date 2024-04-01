
use sql_parser_and_converter::parser; // Adjust the path according to your crate's name
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Debug, Serialize, Deserialize)]
struct TestCase {
    query: String,
    expected_ast: String,
    expected_substrait: String,
}

#[test]
fn test_sql_to_ast_parsing() {
    let test_cases_path = Path::new("../queries.json"); // Adjust the path as needed
    let contents = fs::read_to_string(test_cases_path).expect("file not found");

    let test_cases: Vec<TestCase> = serde_json::from_str(&contents).expect("JSON was not well-formatted");

    for case in test_cases {
        match parser::parse_handler(&case.query) {
            Ok((remaining, ast)) => {
                let ast_json = serde_json::to_string(&ast).expect("Failed to serialize AST");
                assert_eq!(
                    ast_json, case.expected_ast,
                    "AST does not match for query: {}",
                    case.query
                );
                println!(
                    "Query: {}\nParsed AST: {:?}, Remaining: '{}'",
                    case.query, ast, remaining
                );
            }
            Err(e) => panic!("Failed to parse query: {}\n  {:?}", case.query, e),
        }
    }
}