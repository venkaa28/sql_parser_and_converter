use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    CreateTable(CreateTableStatement),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct SelectStatement {
    pub columns: Vec<Column>,
    pub from: FromClause,
    pub join: Option<JoinClause>,
    pub where_clause: Option<WhereClause>,
    pub group_by: Option<Vec<Column>>,
    pub limit: Option<i32>,
    pub end_of_statement: Option<char>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct InsertStatement {
    pub target_table: Table,
    pub source: Box<Statement>, // Simplification, assuming INSERT FROM SELECT only
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct CreateTableStatement {
    pub table_name: Table,
    pub columns: Vec<ColumnDefinition>,
    pub primary_key: String, // Simplification, assuming single column PK for now
    pub end_of_statement: Option<char>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Column {
    Star,
    Name(String),
    Function(Function),
    Number(i32),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Function {
    pub func: AggregateFunction,
    pub val: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum AggregateFunction {
    Count,
    // Extend with other aggregate functions as needed
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct FromClause {
    pub table: Table,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct JoinClause {
    pub table: Table,
    pub condition: Condition,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub alias: Option<String>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct WhereClause {
    pub condition: Condition,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Condition {
    GreaterThan { column: Column, value: i32 },
    EqualTo { val1: Column, val2: Column },
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum DataType {
    Int,
    UInt64,
    String,
    DateTime,
    Decimal(u8, u8), // Precision, Scale
}
