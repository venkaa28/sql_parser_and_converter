mod parser;

fn main() {
    let query_1 = "SELECT COUNT(*) FROM hits;";
    println!("{}", query_1);
    println!("");
    match parser::parse_handler(query_1) {
        Ok((remaining, ast)) => {
            println!("Parsed AST: {:?}, Remaining: '{}'", ast, remaining);
        },
        Err(e) => println!("No valid SQL query found\n  {:?}", e),
    }
    println!("");

    let query_2 = "SELECT \"user_id\", COUNT(*) FROM hits WHERE \"clicks\" > 0 GROUP BY \"user_id\";";
    println!("{}", query_2);
    println!("");
    match parser::parse_handler(query_2) {
        Ok((remaining, ast)) => {
            println!("Parsed AST: {:?}, Remaining: '{}'", ast, remaining);
        },
        Err(e) => println!("No valid SQL query found\n  {:?}", e),
    }
    println!("");

    let query_3 = "SELECT 1, \"URL\" FROM hits LIMIT 10;";
    println!("{}", query_3);
    match parser::parse_handler(query_3) {
        Ok((remaining, ast)) => {
            println!("Parsed AST: {:?}, Remaining: '{}'", ast, remaining);
        },
        Err(e) => println!("No valid SQL query found\n  {:?}", e),
    }
    println!("");

    let query_4 = "INSERT INTO hits SELECT * FROM partial_hits_1 AS p1 JOIN partial_hits_2 AS p2 ON p1.id = p2.id;";
    println!("{}", query_4);
    match parser::parse_handler(query_4) {
        Ok((remaining, ast)) => {
            println!("Parsed AST: {:?}, Remaining: '{}'", ast, remaining);
        },
        Err(e) => println!("No valid SQL query found\n  {:?}", e),
    }
    println!("");
}
