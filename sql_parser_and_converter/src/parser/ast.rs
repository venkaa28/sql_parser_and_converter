#[derive(Debug, PartialEq)]
pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    CreateTable(CreateTableStatement),
}

#[derive(Debug, PartialEq)]
pub struct SelectStatement {
    pub columns: Vec<Column>,
    pub from: String,
    pub where_clause: Option<WhereClause>,
    pub group_by: Vec<Column>,
    pub limit: Option<u32>,
    pub end_of_statement: Option<char>,
}

#[derive(Debug, PartialEq)]
pub struct InsertStatement {
    pub target_table: String,
    pub source: Box<Statement>, // Simplification, assuming INSERT FROM SELECT only
}

#[derive(Debug, PartialEq)]
pub struct CreateTableStatement {
    pub table_name: String,
    pub columns: Vec<ColumnDefinition>,
    pub primary_key: Vec<String>, // Simplification, assuming single column PK for now
}

#[derive(Debug, PartialEq)]
pub enum Column {
    Star,
    Name(String),
    Function(Function),
}
#[derive(Debug, PartialEq)]
pub struct Function {
    pub func: AggregateFunction,
    pub val: String
}

#[derive(Debug, PartialEq)]
pub enum AggregateFunction {
    Count,
    // Extend with other aggregate functions as needed
}

#[derive(Debug, PartialEq)]
pub struct WhereClause {
    pub condition: Condition,
}

#[derive(Debug, PartialEq)]
pub enum Condition {
    GreaterThan { column: String, value: i32 }, // Simplification for the example
    // Add other conditions as needed
}

#[derive(Debug, PartialEq)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, PartialEq)]
pub enum DataType {
    Int,
    UInt64,
    String,
    DateTime,
    Decimal(u8, u8), // Precision, Scale
    // Extend with other data types as needed
}