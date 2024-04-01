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
            map(
                tuple((
                    parse_number,              // Parse precision as u8
                    char(','),             // Expect a comma separator
                    parse_number,              // Parse scale as u8
                )),
                |(precision, _, scale)| {
                    // Attempt to cast precision and scale to u8
                    match (precision.try_into(), scale.try_into()) {
                        (Ok(precision_u8), Ok(scale_u8)) => DataType::Decimal(precision_u8, scale_u8),
                        _ => panic!("Precision or scale value out of u8 range"), // Handle error appropriately
                    }
                },
            ),
            char(')')
        ),
    ))(input)
}

fn parse_primary_key(input: &str) -> IResult<&str, Option<&str>> {
    opt(preceded(
        |input| parse_keyword(input, "PRIMARY KEY"),
        parse_alias
    ))(input)
}