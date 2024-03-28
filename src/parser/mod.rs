use nom::{
    branch::alt,
    character::complete::{alpha1, alphanumeric1, multispace0, multispace1, char, none_of, digit1},
    combinator::{map, map_res, recognize, opt, value},
    sequence::{delimited, pair, preceded, tuple, terminated},
    IResult, 
    bytes::complete::{tag_no_case, tag, escaped_transform},
    multi::{many0, separated_list0, separated_list1}
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
fn parse_table_name(input: &str) -> IResult<&str, Table> {
    map(
        tuple((
            // Parse the table name
            recognize(pair(
                alt((alpha1, map(char('_'), |_| "_"))),
                many0(alt((alphanumeric1, map(char('_'), |_| "_")))),
            )),
            // Parse an optional alias
            opt(preceded(
                |input| parse_keyword(input, "AS"),
                parse_alias
            )),
        )),
        |(name, alias): (&str, Option<&str>)| Table {
            name: name.to_string(),
            alias: alias.map(String::from),
        },
    )(input)
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
fn parse_from_clause(input: &str) -> IResult<&str, FromClause> {
    let (input, _) = parse_keyword(input, "FROM")?;
    let (input, table) = parse_table_name(input)?;

    Ok((input, FromClause { table }))
}

// Parses an alias name
fn parse_alias(input: &str) -> IResult<&str, &str> {
    recognize(pair(
        alt((alpha1, map(char('_'), |_| "_"))), 
        many0(alt((alphanumeric1, map(char('_'), |_| "_")))),
    ))(input)
}

fn parse_condition(input: &str) -> IResult<&str, Condition> {
    alt((
        // Parse "Greater Than" condition
        map(
            tuple((
                parse_column_name,
                multispace0,
                char('>'),
                multispace0,
                parse_number,
            )),
            |(column, _, _, _, value)| Condition::GreaterThan { column, value },
        ),
        // Parse "Equal To" condition
        map(
            tuple((
                parse_column_name,
                multispace0,
                char('='),
                multispace0,
                parse_column_name, // Assuming you want to compare two column names for equality
            )),
            |(val1, _, _, _, val2)| Condition::EqualTo { val1, val2 },
        ),
    ))(input)
}

fn parse_where_clause(input: &str) -> IResult<&str, Option<WhereClause>> {
    opt(preceded(
        |input| parse_keyword(input, "WHERE"),
        map(
            parse_condition,
            |condition| WhereClause { condition },
        ),
    ))(input)
}

fn parse_join_clause(input: &str) -> IResult<&str, Option<JoinClause>> {
    opt(map(
        tuple((
            preceded(|input| parse_keyword(input, "JOIN"), parse_table_name),
            preceded(|input| parse_keyword(input, "ON"), parse_condition),
        )),
        |(table, condition)| JoinClause {
            table,
            condition,
        }
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
    opt(preceded(
        |input| parse_keyword(input, "LIMIT"),
        parse_number
    ))(input)
}

/// Parses an optional end of statement character (';'), preceded by zero or more whitespace characters.
fn parse_end_of_statement(input: &str) -> IResult<&str, Option<char>> {
    opt(preceded(multispace0, char(';')))(input)
}

fn parse_select_statement(input: &str) -> IResult<&str, Statement> {
    let (input, columns) = parse_selections(input)?;
    let (input, from) = parse_from_clause(input)?;
    let (input, join) = parse_join_clause(input)?;
    let (input, where_clause) = parse_where_clause(input)?;
    let (input, group_by) = parse_group_by_clause(input)?;
    let (input, limit) = parse_limit_clause(input)?;
    let (input, end_of_statement) = parse_end_of_statement(input)?;
    Ok((
        input,
        Statement::Select(SelectStatement {
            columns,
            from,
            join, 
            where_clause, 
            group_by, 
            limit, // Placeholder: implement actual parsing
            end_of_statement,
        },
    )))
}

fn parse_insert_statement(input: &str) -> IResult<&str, Statement> {
    let (input, target_table) = parse_table_name(input)?;
    // known bug this will succeed if the nested query is another INSERT INTO or CREATE TABLE
    let (input, source) = parse_handler(input)?;
    Ok((
        input,
        Statement::Insert(InsertStatement {
            target_table,
            source: Box::new(source)
        },
    )))
}

// fn parse_create_table_statement(input: &str) -> IResult<&str, Statement> {
//     let (input, table_name) = parse_table_name(input)?;
//     let (input, columns) = parse_columns(input)?;
//     let (input, primary_key) = parse_primary_key(input)?;
//     Ok((
//         input,
//         Statement::CreateTable(CreateTableStatement {
//             table_name,
//             columns,
//             primary_key: primary_key.unwrap().to_string()
//         },
//     )))
// }

// Revised parse_handler function
pub fn parse_handler(input: &str) -> IResult<&str, Statement> {
    alt((
        preceded(|i| parse_keyword(i, "SELECT"), parse_select_statement),
        preceded(|i| parse_keyword(i, "INSERT INTO"), parse_insert_statement),
        // preceded(|i| parse_keyword(i, "CREATE TABLE"), parse_create_table_statement),
    ))(input)
}