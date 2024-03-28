mod parser;

fn main() {
    let query_1 = "SELECT COUNT(*) FROM hits;";
    println!("{}", query_1);
    let query_2 = "SELECT \"user_id\", COUNT(*) FROM hits WHERE \"clicks\" > 0 GROUP BY \"user_id\";";
    println!("{}", query_2);
    let query_3 = "SELECT 1, \"URL\" FROM hits LIMIT 10;";
    println!("{}", query_2);

    // Example usage of the parse_handler function.
    match parser::parse_handler(query_1) {
        Ok((remaining, ast)) => {
            println!("Parsed AST: {:?}, Remaining: '{}'", ast, remaining);
        },
        Err(_) => println!("No valid SQL query found"),
    }

    // match parser::parse_select_query(query_1) {
    //     Ok((remaining, ast)) => {
    //         println!("Parsed AST: {:?}, Remaining: '{}'", ast, remaining);
    //     },
    //     Err(e) => println!("Error: {:?}", e),
    // }

   
    // match parser::parse_select_query(query_2) {
    //     Ok((remaining, ast)) => {
    //         println!("Parsed AST: {:?}, Remaining: '{}'", ast, remaining);
    //     },
    //     Err(e) => println!("Error: {:?}", e),
    // }

    // match parser::parse_select_query(query_3) {
    //     Ok((remaining, ast)) => {
    //         println!("Parsed AST: {:?}, Remaining: '{}'", ast, remaining);
    //     },
    //     Err(e) => println!("Error: {:?}", e),
    // }
}
