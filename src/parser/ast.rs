#[derive(Debug, PartialEq)]
pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    CreateTable(CreateTableStatement),
}

#[derive(Debug, PartialEq)]
pub struct SelectStatement {
    pub columns: Vec<Column>,
    pub from: FromClause,
    pub join: Option<JoinClause>,
    pub where_clause: Option<WhereClause>,
    pub group_by: Option<Vec<Column>>,
    pub limit: Option<i32>,
    pub end_of_statement: Option<char>,
}

#[derive(Debug, PartialEq)]
pub struct InsertStatement {
    pub target_table: Table,
    pub source: Box<Statement>, // Simplification, assuming INSERT FROM SELECT only
}

#[derive(Debug, PartialEq)]
pub struct CreateTableStatement {
    pub table_name: Table,
    pub columns: Vec<ColumnDefinition>,
    pub primary_key: String, // Simplification, assuming single column PK for now
}

#[derive(Debug, PartialEq)]
pub enum Column {
    Star,
    Name(String),
    Function(Function),
    Number(i32)
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
pub struct FromClause {
   pub table: Table
}

#[derive(Debug, PartialEq)]
pub struct JoinClause {
    pub table: Table,
    pub condition: Condition
}

#[derive(Debug, PartialEq)]
pub struct Table {
    pub name: String,
    pub alias: Option<String>
}

#[derive(Debug, PartialEq)]
pub struct WhereClause {
    pub condition: Condition,
}

#[derive(Debug, PartialEq)]
pub enum Condition {
    GreaterThan { column: Column, value: i32 }, 
    EqualTo {val1: Column, val2: Column}
}

#[derive(Debug, PartialEq)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, PartialEq, Clone)]
pub enum DataType {
    Int,
    UInt64,
    String,
    DateTime,
    Decimal(u8, u8), // Precision, Scale
    // Extend with other data types as needed
}