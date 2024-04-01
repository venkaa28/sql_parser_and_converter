use serde_json::json;

mod binder;
mod parser;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let query_1 = "SELECT COUNT(*) FROM hits;";
    println!("{}", query_1);
    println!("");
    match parser::parse_handler(query_1) {
        Ok((remaining, ast)) => {
            println!("Parsed AST: {:?}, Remaining: '{}'", ast, remaining);
            //ast_q1 = Some(ast);
        }
        Err(e) => println!("No valid SQL query found\n  {:?}", e),
    }
    println!("");

    let query_2 =
        "SELECT \"user_id\", COUNT(*) FROM hits WHERE \"clicks\" > 0 GROUP BY \"user_id\";";
    println!("{}", query_2);
    println!("");
    match parser::parse_handler(query_2) {
        Ok((remaining, ast)) => {
            println!("Parsed AST: {:?}, Remaining: '{}'", ast, remaining);
        }
        Err(e) => println!("No valid SQL query found\n  {:?}", e),
    }
    println!("");

    let query_3 = "SELECT 1, \"URL\" FROM hits LIMIT 10;";
    println!("{}", query_3);
    println!("");
    match parser::parse_handler(query_3) {
        Ok((remaining, ast)) => {
            println!("Parsed AST: {:?}, Remaining: '{}'", ast, remaining);
        }
        Err(e) => println!("No valid SQL query found\n  {:?}", e),
    }
    println!("");

    let query_4 = "INSERT INTO hits SELECT * FROM partial_hits_1 AS p1 JOIN partial_hits_2 AS p2 ON p1.id = p2.id;";
    println!("{}", query_4);
    println!("");
    match parser::parse_handler(query_4) {
        Ok((remaining, ast)) => {
            println!("Parsed AST: {:?}, Remaining: '{}'", ast, remaining);
        }
        Err(e) => println!("No valid SQL query found\n  {:?}", e),
    }
    println!("");

    let query_5 = "CREATE TABLE hits (id UInt64, timestamp DateTime, user_id UInt64, event_type String, url String, session_id String, user_agent String, country String, ip String, referrer String, clicks UInt64, impressions UInt64, revenue Decimal(10, 2)) PRIMARY KEY id;";
    println!("{}", query_5);
    println!("");
    match parser::parse_handler(query_5) {
        Ok((remaining, ast)) => {
            println!("Parsed AST: {:?}, Remaining: '{}'", ast, remaining);
        }
        Err(e) => println!("No valid SQL query found\n  {:?}", e),
    }
    println!("");

    match parser::parse_handler(query_1) {
        Ok((remaining, ast)) => {
            match binder::ast_to_substrait_plan(&ast) {
                Ok(plan) => {
                    // Now `plan` is unwrapped and can be used directly
                    println!(
                        "Substrait JSON Plan: {:?}",
                        serde_json::to_string_pretty(&plan).unwrap()
                    );
                    // If you want to serialize `plan` to JSON, you can do it here directly
                }
                Err(e) => println!("Failed to convert AST to Substrait plan: {:?}", e),
            }
        }
        Err(e) => println!("Failed to parse query: {:?}", e),
    }

    println!("");

    match parser::parse_handler(query_2) {
        Ok((remaining, ast)) => {
            match binder::ast_to_substrait_plan(&ast) {
                Ok(plan) => {
                    // Now `plan` is unwrapped and can be used directly
                    println!(
                        "Substrait JSON Plan: {:?}",
                        serde_json::to_string_pretty(&plan).unwrap()
                    );
                    // If you want to serialize `plan` to JSON, you can do it here directly
                }
                Err(e) => println!("Failed to convert AST to Substrait plan: {:?}", e),
            }
        }
        Err(e) => println!("Failed to parse query: {:?}", e),
    }

    println!("");

    match parser::parse_handler(query_3) {
        Ok((remaining, ast)) => {
            match binder::ast_to_substrait_plan(&ast) {
                Ok(plan) => {
                    // Now `plan` is unwrapped and can be used directly
                    println!(
                        "Substrait JSON Plan: {:?}",
                        serde_json::to_string_pretty(&plan).unwrap()
                    );
                    // If you want to serialize `plan` to JSON, you can do it here directly
                }
                Err(e) => println!("Failed to convert AST to Substrait plan: {:?}", e),
            }
        }
        Err(e) => println!("Failed to parse query: {:?}", e),
    }

    Ok(())
}
