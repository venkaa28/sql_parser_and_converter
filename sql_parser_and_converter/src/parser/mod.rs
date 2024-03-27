use nom::{
    IResult,
    sequence::{delimited, preceded, tuple, terminated},
    bytes::complete::{tag_no_case, tag, take_until},
    character::complete::{multispace1, multispace0, char, alphanumeric1, digit1},
    combinator::{opt, map},
    branch::alt,
    multi::separated_list0,
};

mod ast;
use ast::*;


//Function to parse Select Keyword
pub fn parse_select_keyword(input: &str) -> IResult<&str, &str> {
    let (input, _) = multispace0(input)?; // Optionally consume leading whitespace
    let (input, _) = tag_no_case("SELECT")(input)?;
    let (input, _) = multispace1(input)?; // Expect at least some whitespace after "SELECT"
    // Continue with parsing the rest of the SELECT statement
    Ok((input, "SELECT"))
}

//Function to parse the aggregate expression within a sql function, currently only handles * and column names
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
    let (input, _) = multispace0(input)?;
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
//function that parses the FROM keyword and stores the value following it as a string
pub fn parse_from_clause(input: &str) -> IResult<&str, String> {
    let (input, _) = tuple((
        multispace1,
        tag_no_case("FROM"),
        multispace1,
    ))(input)?;
    
    // Use the map combinator to convert &str to String
    map(
        alphanumeric1,
        |s: &str| s.to_string(),
    )(input)
}

fn parse_quoted_identifier(input: &str) -> IResult<&str, String> {
    map(
        delimited(char('"'), take_until("\""), char('"')),
        |name: &str| name.to_string(),
    )(input)
}

fn parse_condition_value(input: &str) -> IResult<&str, i32> {
    map(
        delimited(multispace0, digit1, multispace0), // Use `digit1` to parse the number directly
        |digits: &str| digits.parse::<i32>().unwrap(),
    )(input)
}

pub fn parse_where_clause(input: &str) -> IResult<&str, Option<WhereClause>> {
    let parse_condition = map(
        tuple((
            parse_quoted_identifier, // Updated to handle quoted identifiers
            multispace0,
            char('>'),
            multispace0,
            parse_condition_value, // Updated to parse the condition value
        )),
        |(column, _, _, _, value)| WhereClause {
            condition: Condition::GreaterThan { column, value },
        },
    );

    opt(preceded(tuple((multispace0, tag_no_case("WHERE"), multispace0)), parse_condition))(input)
}

// Function to parse the LIMIT clause
pub fn parse_limit_clause(input: &str) -> IResult<&str, Option<u32>> {
    opt(preceded(
        tuple((multispace1, tag_no_case("LIMIT"), multispace1)),
        map(alphanumeric1, |s: &str| s.parse::<u32>().unwrap()), // Parses the limit value
    ))(input)
}

// Function to parse individual column names
fn parse_column_name(input: &str) -> IResult<&str, Column> {
    alt((
        // Handles quoted column names
        map(
            delimited(char('"'), take_until("\""), char('"')), 
            |name: &str| Column::Name(name.to_string())
        ),
        map(delimited(char('\''), alphanumeric1, char('\'')), |name: &str| Column::Name(name.to_string())),
        // Existing handling for unquoted column names
        map(alphanumeric1, |name: &str| Column::Name(name.to_string())),
    ))(input)
}

pub fn parse_group_by_clause(input: &str) -> IResult<&str, Vec<Column>> {
    preceded(
        // Look for the "GROUP BY" keywords with surrounding whitespace
        tuple((multispace0, tag_no_case("GROUP BY"), multispace1)),
        // Parse a list of column names separated by commas
        separated_list0(
            preceded(multispace0, char(',')), // Handle optional whitespace before each comma
            preceded(multispace0, parse_column_name), // Handle optional whitespace before each column name
        )
    )(input)
}

fn parse_selections(input: &str) -> IResult<&str, Vec<Column>> {
    separated_list0(
        preceded(multispace0, terminated(char(','), multispace0)),
        alt((
            parse_count_function, // Make sure this can now return Column directly
            parse_column_name,
        )),
    )(input)
}

pub fn parse_select_query(input: &str) -> IResult<&str, Statement> {
    let (input, _) = parse_select_keyword(input)?;
    let (input, columns) = parse_selections(input)?;
    let (input, from) = parse_from_clause(input)?;
    let (input, where_clause) = parse_where_clause(input)?;
    let (input, group_by) = opt(parse_group_by_clause)(input)?; 
    let (input, limit) = parse_limit_clause(input)?;
    let (input, end_of_statement) = opt(preceded(multispace0, char(';')))(input)?;
    Ok((input, Statement::Select(SelectStatement {
        columns,
        from: from.to_string(),
        where_clause,
        group_by: group_by.unwrap_or_default(), // Handling None case
        limit,
        end_of_statement
    })))
}
