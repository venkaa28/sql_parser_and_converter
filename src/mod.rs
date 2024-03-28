// Parser for the entire columns section
fn parse_columns(input: &str) -> IResult<&str, Vec<ColumnDefinition>> {
    delimited(
        char('('),
        separated_list1(
            tuple((multispace0, char(','), multispace0)),
            parse_column_def,
        ),
        preceded(multispace0, char(')'))
    )(input)
}

// Parser for a single column definition
fn parse_column_def(input: &str) -> IResult<&str, ColumnDefinition> {
    map(
        tuple((
            parse_alias,
            multispace1,
            parse_data_type,
        )),
        |(name, _, data_type)| ColumnDefinition { name: name.to_string(), data_type }
    )(input)
}

// Parser for the DataType enum
fn parse_data_type(input: &str) -> IResult<&str, DataType> {
    alt((
        value(DataType::Int, tag("Int")),
        value(DataType::UInt64, tag("UInt64")),
        value(DataType::String, tag("String")),
        value(DataType::DateTime, tag("DateTime")),
        preceded(
            tag("Decimal("),
            map_res(
                tuple((
                    digit1,
                    char(','),
                    digit1,
                    char(')')
                )),
                |(precision_str, _, scale_str, _)| -> Result<DataType, nom::Err<(&str, nom::error::ErrorKind)>> {
                    let precision = precision_str.parse::<u8>().map_err(|_| nom::Err::Error(ParseError::from_error_kind(input, nom::error::ErrorKind::Digit)))?;
                    let scale = scale_str.parse::<u8>().map_err(|_| nom::Err::Error(ParseError::from_error_kind(input, nom::error::ErrorKind::Digit)))?;
                    Ok(DataType::Decimal(precision, scale))
                }
            )
        ),
    ))(input)
}

fn parse_primary_key(input: &str) -> IResult<&str, Option<&str>> {
    opt(preceded(
        |input| parse_keyword(input, "PRIMARY KEY"),
        parse_alias
    ))(input)
}