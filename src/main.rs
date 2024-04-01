

mod binder;
mod parser;
mod tests;

fn main() -> Result<(), Box<dyn std::error::Error>> {

    let query_1 = "SELECT COUNT(*) FROM hits;";
    println!("Query 1: {}", query_1);
    println!("");
    match parser::parse_handler(query_1) {
        Ok((_remaining, ast)) => {
            let serialized = serde_json::to_string_pretty(&ast)?;
            println!("Query 1: Serialized AST: {}", serialized);
            println!("");
            match binder::ast_to_substrait_plan(&ast) {
                Ok(plan) => {
                    println!(
                        "Query 1: Substrait JSON Plan: {}",
                        serde_json::to_string_pretty(&plan).unwrap()
                    );
                }
                Err(e) => println!("Failed to convert AST to Substrait plan: {:?}", e),
            }
        }
        Err(e) => println!("Failed to parse query: {:?}", e),
    }
    println!("");

    let query_2 =
    "SELECT \"user_id\", COUNT(*) FROM hits WHERE \"clicks\" > 0 GROUP BY \"user_id\";";
    println!("Query 2: {}", query_2);
    println!("");
    match parser::parse_handler(query_2) {
        Ok((_remaining, ast)) => {
            let serialized = serde_json::to_string_pretty(&ast)?;
            println!("Query 2: Serialized AST: {}", serialized);
            println!("");
            match binder::ast_to_substrait_plan(&ast) {
                Ok(plan) => {
                    println!(
                        "Query 2: Substrait JSON Plan: {}",
                        serde_json::to_string_pretty(&plan).unwrap()
                    );
                }
                Err(e) => println!("Failed to convert AST to Substrait plan: {:?}", e),
            }
        }
        Err(e) => println!("Failed to parse query: {:?}", e),
    }
    println!("");

    let query_3 = "SELECT 1, \"URL\" FROM hits LIMIT 10;";
    println!("Query 3: {}", query_3);
    println!("");
    match parser::parse_handler(query_3) {
        Ok((_remaining, ast)) => {
            let serialized = serde_json::to_string_pretty(&ast)?;
            println!("Query 3: Serialized AST: {}", serialized);
            println!("");
            match binder::ast_to_substrait_plan(&ast) {
                Ok(plan) => {
                    println!(
                        "Query 3: Substrait JSON Plan: {}",
                        serde_json::to_string_pretty(&plan).unwrap()
                    );
                }
                Err(e) => println!("Failed to convert AST to Substrait plan: {:?}", e),
            }
        }
        Err(e) => println!("Failed to parse query: {:?}", e),
    }
    println!("");

    let query_4 = "INSERT INTO hits SELECT * FROM partial_hits_1 AS p1 JOIN partial_hits_2 AS p2 ON p1.id = p2.id;";
    println!("Query 4: {}", query_4);
    println!("");
    match parser::parse_handler(query_4) {
        Ok((_remaining, ast)) => {
            let serialized = serde_json::to_string_pretty(&ast)?;
            println!("Query 4: Serialized AST: {}", serialized);
            println!("");
            match binder::ast_to_substrait_plan(&ast) {
                Ok(plan) => {
                    println!(
                        "Query 4: Substrait JSON Plan: {}",
                        serde_json::to_string_pretty(&plan).unwrap()
                    );
                }
                Err(e) => println!("Failed to convert AST to Substrait plan: {:?}", e),
            }
        }
        Err(e) => println!("Failed to parse query: {:?}", e),
    }
    println!("");

    let query_5 = "CREATE TABLE hits (id UInt64, timestamp DateTime, user_id UInt64, event_type String, url String, session_id String, user_agent String, country String, ip String, referrer String, clicks UInt64, impressions UInt64, revenue Decimal(10, 2)) PRIMARY KEY id;";
    println!("Query 5: {}", query_5);
    println!("");
    match parser::parse_handler(query_5) {
        Ok((_remaining, ast)) => {
            let serialized = serde_json::to_string_pretty(&ast)?;
            println!("Query 5: Serialized AST: {}", serialized);
            println!("");
            match binder::ast_to_substrait_plan(&ast) {
                Ok(plan) => {
                    println!(
                        "Query 5: Substrait JSON Plan: {}",
                        serde_json::to_string_pretty(&plan).unwrap()
                    );
                }
                Err(e) => println!("Failed to convert AST to Substrait plan: {:?}", e),
            }
        }
        Err(e) => println!("Failed to parse query: {:?}", e),
    }
    println!("");


    let fail_query = "SELECT SELECT COUNT(*) FROM hits;";
    println!("Fail Query: {}", fail_query);
    println!("");
    match parser::parse_handler(fail_query) {
        Ok((_remaining, ast)) => {
            let serialized = serde_json::to_string_pretty(&ast)?;
            println!("Fail Query: Serialized AST: {}", serialized);
            println!("");
            match binder::ast_to_substrait_plan(&ast) {
                Ok(plan) => {
                    println!(
                        "Fail Query: Substrait JSON Plan: {}",
                        serde_json::to_string_pretty(&plan).unwrap()
                    );
                }
                Err(e) => println!("Failed to convert AST to Substrait plan: {:?}", e),
            }
        }
        Err(e) => println!("Failed to parse query: {:?}", e),
    }
    println!("");

    Ok(())
}
