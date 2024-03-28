use nom::{
    branch::alt,
    character::complete::{alpha1, alphanumeric1, multispace0, char, none_of, digit1},
    combinator::{map, map_res, recognize, opt},
    sequence::{delimited, pair, preceded, tuple, terminated},
    IResult, 
    bytes::complete::{tag_no_case, tag, escaped_transform},
    multi::{many0, separated_list0}
};

mod ast;
use ast::*;

// Parses static SQL keywords with surrounding optional whitespaces.
fn parse_keyword<'a>(input: &'a str, keyword: &'static str) -> IResult<&'a str, &'a str> {
    delimited(multispace0, tag_no_case(keyword), multispace0)(input)
}

/// Parses the '*' character.
fn parse_star(input: &str) -> IResult<&str, Column> {
    map(char('*'), |_| Column::Star)(input)
}

/// Parses an integer number.
fn parse_number(input: &str) -> IResult<&str, i32> {
    map_res(
        digit1,
        |digit_str: &str| digit_str.parse::<i32>()
    )(input)
}

/// Parses a string literal that is specifically a "word" enclosed in double quotes,
/// correctly handling escaped double quotes within.
fn parse_column_name(input: &str) -> IResult<&str, Column> {
    map(
        delimited(
        tag("\""),
        // Process the content between the double quotes.
        // Here, we only consider the escaped double quote sequence.
        escaped_transform(
            // Take characters that are not a backslash or double quote.
            // This effectively allows any character except for control sequences.
            none_of("\\\""),
            '\\',
            // Define handling of the escaped double quote.
            map(tag("\""), |_| "\""),
        ),
        tag("\"")
    ),
    |name: String| Column::Name(name)
    )(input)
}

/// Parses a table name.
fn parse_table_name(input: &str) -> IResult<&str, &str> {
    recognize(pair(
        alt((alpha1, map(char('_'), |_| "_"))), 
        many0(alt((alphanumeric1, map(char('_'), |_| "_")))),
    ))(input)
}

fn parse_aggregate_expression(input: &str) -> IResult<&str, String> {
    alt((
        map(tag("*"), |_| "*".to_string()), // Handle the '*' character
        map(alphanumeric1, |s: &str| s.to_string()), // Handle column names
    ))(input)
}

//function to parse the function count
pub fn parse_count_function(input: &str) -> IResult<&str, Column> {
    //inline function discards first and third objects. calls parse_aggregate_expression function to grab the value within the parenthesis
    let mut parse_count_contents = delimited(char('('), parse_aggregate_expression, char(')'));
    // looks for the word count, the calls the function described above
    let (input, _) = tag_no_case("COUNT")(input)?;
    // let (input, _) = multispace0(input)?;
    let (input, val) = parse_count_contents(input)?;
    //creates a column object using the count function and the value of its aggregate expression
    Ok((
        input, 
        Column::Function(Function { 
            func: AggregateFunction::Count, 
            val 
        })
    ))
}

/// Parses the selections of a SQL SELECT statement, allowing either a single '*' or
/// a series of comma-separated columns and COUNT functions.
fn parse_selections(input: &str) -> IResult<&str, Vec<Column>> {
    alt((
        // Branch to parse a single '*' as the only selection.
        map(parse_star, |star| vec![star]),
        // Branch to parse a list of comma-separated columns and COUNT functions.
        separated_list0(
            preceded(multispace0, terminated(char(','), multispace0)),
            alt((
                parse_column_name,
                parse_count_function,
                map(parse_number, Column::Number)
            )),
        ),
    ))(input)
}

/// Parses the FROM clause of a SQL query, extracting the table name.
fn parse_from_clause(input: &str) -> IResult<&str, String> {
    let (input, (_, table_name)) = tuple((
        |input| parse_keyword(input, "FROM"),
        parse_table_name,
    ))(input)?;
    Ok((input, table_name.to_string()))
}

fn parse_where_clause(input: &str) -> IResult<&str, Option<WhereClause>> {
     //Can refactor to account for different conditions, hard coded > for simplicity
    // Attempt to parse a WHERE clause, if present
    opt(preceded(
        // Match the "WHERE" keyword with surrounding optional whitespaces
        |input| parse_keyword(input, "WHERE"),
        // Parse the condition following the "WHERE" keyword
        map(
            tuple((
                parse_column_name, // Parses the column name enclosed in quotes
                multispace0,       // Optional spaces
                char('>'),         // The '>' character for the "greater than" condition
                multispace0,       // Optional spaces after '>'
                parse_number,      // Parses the number following '>'
            )),
            // Construct a WhereClause from the parsed components
            |(column, _, _, _, value)| WhereClause {
                condition: Condition::GreaterThan {
                    column,
                    value,
                },
            },
        ),
    ))(input)
}

fn parse_group_by_clause(input: &str) -> IResult<&str, Option<Vec<Column>>> {
    opt(preceded(
        |input| parse_keyword(input, "GROUP BY"),
        separated_list0(
            preceded(multispace0, terminated(char(','), multispace0)),
            parse_column_name
        )
    ))(input)
}

fn parse_limit_clause(input: &str) -> IResult<&str, Option<i32>> {
    opt((preceded(
        |input| parse_keyword(input, "LIMIT"),
        parse_number
    )))(input)
}

/// Parses an optional end of statement character (';'), preceded by zero or more whitespace characters.
fn parse_end_of_statement(input: &str) -> IResult<&str, Option<char>> {
    opt(preceded(multispace0, char(';')))(input)
}

fn parse_select_statement(input: &str) -> IResult<&str, Statement> {
    let (input, columns) = parse_selections(input)?;
    let (input, from) = parse_from_clause(input)?;
    let (input, where_clause) = parse_where_clause(input)?;
    let (input, group_by) = parse_group_by_clause(input)?;
    let (input, limit) = parse_limit_clause(input)?;
    let (input, end_of_statement) = parse_end_of_statement(input)?;
    Ok((
        input,
        Statement::Select(SelectStatement {
            columns,
            from, 
            where_clause, 
            group_by, 
            limit, // Placeholder: implement actual parsing
            end_of_statement,
        },
    )))
}

fn parse_insert_statement(input: &str) -> IResult<&str, String> {
    Ok((input, "Parsed INSERT INTO statement".to_string()))
}

fn parse_create_table_statement(input: &str) -> IResult<&str, String> {
    Ok((input, "Parsed CREATE TABLE statement".to_string()))
}

// Revised parse_handler function
pub fn parse_handler(input: &str) -> IResult<&str, Statement> {
    alt((
        preceded(|i| parse_keyword(i, "SELECT"), parse_select_statement),
        // |i| parse_keyword(i, "INSERT INTO").and_then(|(next_input, _)| parse_insert_statement(next_input)),
        // |i| parse_keyword(i, "CREATE TABLE").and_then(|(next_input, _)| parse_create_table_statement(next_input)),
    ))(input)
}