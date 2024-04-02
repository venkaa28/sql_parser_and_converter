use std::vec;

use substrait::proto::{
    self, aggregate_rel::Grouping, expression::{
        self,
        field_reference::ReferenceType,
        reference_segment::StructField,
        FieldReference, ReferenceSegment, RexType, ScalarFunction,
    }, extensions::{
        self,
        simple_extension_declaration::{ExtensionFunction, MappingType},
        SimpleExtensionDeclaration, SimpleExtensionUri,
    }, function_argument::ArgType, plan_rel::RelType, read_rel, r#type::{self, Kind, Struct}, Expression, FunctionArgument, NamedObjectWrite, NamedStruct, Plan, PlanRel, Rel, RelRoot, Type,
    ddl_rel
};

use crate::parser::ast::*;

fn select_statement_to_substrait(
    select: &SelectStatement,
) -> Result<PlanRel, Box<dyn std::error::Error>> {
    // Construct read relation
    let read_rel = Rel {
        rel_type: Some(proto::rel::RelType::Read(Box::new(proto::ReadRel {
            read_type: Some(read_rel::ReadType::NamedTable(read_rel::NamedTable {
                names: vec![select.from.table.name.clone()],
                advanced_extension: None,
            })),
            base_schema: Some(get_hits_base_schema()),
            ..Default::default()
        }))),
        ..Default::default()
    };

    let mut input_rel = read_rel;

    //if the select statement has a where clause
    if let Some(where_clause) = &select.where_clause {
        if let Condition::GreaterThan { column, value: _ } = &where_clause.condition {
            //putting column in a list of columns -> list of strings -> string -> field index of string in base schema
            let column_slice = std::slice::from_ref(column);
            let field_names = construct_column_names(column_slice);
            let field_name = field_names
                .get(0)
                .map(|name| name.as_str())
                .unwrap_or_default();
            let field_index = get_field_index_in_schema(&get_hits_base_schema(), field_name)
                .expect("Column name not found in schema"); // This should be handled more gracefully

            let condition_expr = Expression {
                    rex_type: Some(expression::RexType::ScalarFunction(ScalarFunction {
                        function_reference: 3, //ref to greater than function
                        arguments: vec![
                            FunctionArgument {
                                arg_type: Some(ArgType::Value(Expression {
                                    rex_type: Some(RexType::Selection(
                                        Box::new(FieldReference {
                                            reference_type: Some(ReferenceType::DirectReference(
                                                ReferenceSegment {
                                                    reference_type: Some(expression::reference_segment::ReferenceType::StructField(
                                                        Box::new(StructField {
                                                            field: field_index as i32,
                                                            child: None,
                                                        })
                                                    )),
                                                }
                                            )),
                                            root_type: None,
                                        })
                                    )),
                                })),
                            },
                            FunctionArgument {
                                arg_type: Some(ArgType::Value(Expression {
                                    rex_type: Some(RexType::Literal(expression::Literal {
                                        nullable: false,
                                        type_variation_reference: 0,
                                        literal_type: Some(expression::literal::LiteralType::I32(0)), // Literal value of 0
                                })),
                                })),
                            }
                        ],
                        ..Default::default()
                    })),
                    ..Default::default()
            };

            // Wrap the in in a Filter relation
            input_rel = Rel {
                rel_type: Some(proto::rel::RelType::Filter(Box::new(proto::FilterRel {
                    input: Some(Box::new(input_rel)),
                    condition: Some(Box::new(condition_expr)),
                    ..Default::default()
                }))),
                ..Default::default()
            };
        }
    }

    //if the select statement has a group by clause
    if let Some(group_by_columns) = &select.group_by {
        let grouping_columns = construct_column_names(&group_by_columns);
        let groupings = grouping_columns
            .into_iter()
            .map(|name| {
                let field_index = get_field_index_in_schema(&get_hits_base_schema(), &name)
                    .expect("Field name not found in schema");
                Expression {
                    rex_type: Some(RexType::Selection(Box::new(FieldReference {
                        reference_type: Some(ReferenceType::DirectReference(ReferenceSegment {
                            reference_type: Some(
                                expression::reference_segment::ReferenceType::StructField(
                                    Box::new(StructField {
                                        field: field_index as i32,
                                        child: None,
                                    }),
                                ),
                            ),
                        })),
                        root_type: None,
                    }))),
                }
            })
            .collect::<Vec<_>>();

        //wrap the input in an aggregate rel
        input_rel = Rel {
            rel_type: Some(proto::rel::RelType::Aggregate(Box::new(
                proto::AggregateRel {
                    input: Some(Box::new(input_rel)), // Wrap the previous input_rel as the input to AggregateRel
                    groupings: vec![Grouping {
                        grouping_expressions: groupings, // Use the constructed groupings
                    }],
                    ..Default::default()
                },
            ))),
        };
    }

    //if the select statement has a limit clause, wrap the input in a Fetch Relation
    if let Some(limit_clause) = &select.limit {
        input_rel = Rel {
            rel_type: Some(proto::rel::RelType::Fetch(Box::new(proto::FetchRel {
                input: Some(Box::new(input_rel)),
                count: *limit_clause as i64,
                ..Default::default()
            }))),
        };
    }

    // Construct the root relation
    let root_rel = RelRoot {
        names: construct_column_names(&select.columns), // Output column names
        input: Some(input_rel),                         // Input relation
        ..Default::default()
    };

    let rel_type = RelType::Root(root_rel);

    // Construct the plan with the root relation
    let plan_rel = PlanRel {
        rel_type: Some(rel_type),
    };

    Ok(plan_rel)
}

//function to turn column names into a vector of strings for the root relations names field
fn construct_column_names(columns: &[Column]) -> Vec<std::string::String> {
    columns
        .iter()
        .map(|column| {
            match column {
                Column::Star => "*".to_string(),
                Column::Name(name) => name.clone(),
                Column::Function(func) => match func.func {
                    AggregateFunction::Count => "count".to_string(),
                    //figure out how to import the count extension from functions_aggregate_generic.yaml and reference the function in the root_rel names field
                },
                Column::Number(num) => num.to_string(),
            }
        })
        .collect()
}

//function got get hits base schema
fn get_hits_base_schema() -> NamedStruct {
    NamedStruct {
        names: vec![
            "id".to_string(),
            "timestamp".to_string(),
            "user_id".to_string(),
            "event_type".to_string(),
            "url".to_string(),
            "session_id".to_string(),
            "user_agent".to_string(),
            "country".to_string(),
            "ip".to_string(),
            "referrer".to_string(),
            "clicks".to_string(),
            "impressions".to_string(),
            "revenue".to_string(),
        ],
        r#struct: Some(Struct {
            types: vec![
                Type {
                    kind: Some(Kind::I64(r#type::I64 {
                        nullability: 1,
                        ..Default::default()
                    })),
                }, // Assuming this represents an "id" field.
                Type {
                    kind: Some(Kind::Timestamp(r#type::Timestamp {
                        nullability: 1,
                        ..Default::default()
                    })),
                }, // Assuming this represents a "timestamp" field.
                Type {
                    kind: Some(Kind::I64(r#type::I64 {
                        nullability: 1,
                        ..Default::default()
                    })),
                }, // Assuming this represents a "user_id" field.
                Type {
                    kind: Some(Kind::String(r#type::String {
                        nullability: 1,
                        ..Default::default()
                    })),
                }, // Assuming this represents an "event_type" field.
                Type {
                    kind: Some(Kind::String(r#type::String {
                        nullability: 1,
                        ..Default::default()
                    })),
                }, // Assuming this represents a "url" field.
                Type {
                    kind: Some(Kind::String(r#type::String {
                        nullability: 1,
                        ..Default::default()
                    })),
                }, // Assuming this represents a "session_id" field.
                Type {
                    kind: Some(Kind::String(r#type::String {
                        nullability: 1,
                        ..Default::default()
                    })),
                }, // Assuming this represents a "user_agent" field.
                Type {
                    kind: Some(Kind::String(r#type::String {
                        nullability: 1,
                        ..Default::default()
                    })),
                }, // Assuming this represents a "country" field.
                Type {
                    kind: Some(Kind::String(r#type::String {
                        nullability: 1,
                        ..Default::default()
                    })),
                }, // Assuming this represents an "ip" field.
                Type {
                    kind: Some(Kind::String(r#type::String {
                        nullability: 1,
                        ..Default::default()
                    })),
                }, // Assuming this represents a "referrer" field.
                Type {
                    kind: Some(Kind::I64(r#type::I64 {
                        nullability: 1,
                        ..Default::default()
                    })),
                }, // Assuming this represents a "clicks" field.
                Type {
                    kind: Some(Kind::I64(r#type::I64 {
                        nullability: 1,
                        ..Default::default()
                    })),
                }, // Assuming this represents an "impressions" field.
                Type {
                    kind: Some(Kind::Decimal(r#type::Decimal {
                        precision: 10,
                        scale: 2,
                        nullability: 1,
                        ..Default::default()
                    })),
                }, // Assuming this represents a "revenue" field.
            ],
            type_variation_reference: 0, // Default value for simplicity
            nullability: 1,              // Assuming the whole struct is required
        }),
    }
}

fn get_field_index_in_schema(schema: &NamedStruct, field_name: &str) -> Option<usize> {
    schema.names.iter().position(|name| name == field_name)
}

fn create_named_struct_from_columns(columns: &[ColumnDefinition]) -> NamedStruct {
    let names = columns.iter().map(|col| col.name.clone()).collect();
    let types = columns.iter().map(|col| {
        match col.data_type {
            DataType::Int | DataType::UInt64 => {
                Type { 
                    kind: Some(Kind::I64(substrait::proto::r#type::I64::default())) // Assuming I64 has a default implementation
                }
            },
            DataType::String => {
                Type { 
                    kind: Some(Kind::String(substrait::proto::r#type::String::default())) // Assuming String has a default implementation
                }
            },
            DataType::DateTime => {
                Type { 
                    kind: Some(Kind::Timestamp(substrait::proto::r#type::Timestamp::default())) // Assuming Timestamp has a default implementation
                }
            },
            DataType::Decimal(precision, scale) => {
                Type { 
                    kind: Some(Kind::Decimal(substrait::proto::r#type::Decimal { 
                        precision: precision as i32, 
                        scale: scale as i32,
                        ..substrait::proto::r#type::Decimal::default() // Assuming additional fields can be filled with defaults
                    })) 
                }
            },
            // Handle other data types
        }
    }).collect();

    NamedStruct {
        names,
        r#struct: Some(Struct {
            types,
            type_variation_reference: 0, // Default for simplicity
            nullability: 1, // Assuming the whole struct is required
        }),
    }
}

fn create_table_statement_to_substrait(
    create: &CreateTableStatement,
) -> Result<PlanRel, Box<dyn std::error::Error>> {


    let ddl_rel = Rel {
        rel_type: Some(proto::rel::RelType::Ddl(Box::new(proto::DdlRel {
            table_schema: Some(create_named_struct_from_columns(&create.columns)),
            object: 1, 
            op: 1, 
            write_type: Some(ddl_rel::WriteType::NamedObject(NamedObjectWrite {
                names: vec![create.table_name.name.clone()],
                advanced_extension: None
            })),
            ..Default::default()
        })))
    };

      // Construct the root relation
      let root_rel = RelRoot {
        input: Some(ddl_rel),
        ..Default::default()
    };

    let rel_type = RelType::Root(root_rel);

    // Construct the plan with the root relation
    let plan_rel = PlanRel {
        rel_type: Some(rel_type),
    };

    Ok(plan_rel)
}

//function to convert any statement into a substrait plan based on the type of Statement it is
pub fn ast_to_substrait_plan(ast: &Statement) -> Result<Plan, Box<dyn std::error::Error>> {
    let extension_uris:Vec<extensions::SimpleExtensionUri> = vec![
        SimpleExtensionUri {
            uri: "https://github.com/substrait-io/substrait/blob/main/extensions/functions_aggregate_generic.yaml".to_string(),
            extension_uri_anchor: 1
        },
        SimpleExtensionUri {
            uri: "https://github.com/substrait-io/substrait/blob/main/extensions/functions_comparison.yaml".to_string(),
            extension_uri_anchor: 2
        }
    ];

    let extensions: Vec<extensions::SimpleExtensionDeclaration> = vec![
        SimpleExtensionDeclaration {
            mapping_type: {
                Some(MappingType::ExtensionFunction(ExtensionFunction {
                    extension_uri_reference: 1,
                    function_anchor: 1,
                    name: "count".to_string(),
                }))
            },
        },
        SimpleExtensionDeclaration {
            mapping_type: {
                Some(MappingType::ExtensionFunction(ExtensionFunction {
                    extension_uri_reference: 2,
                    function_anchor: 2,
                    name: "equal".to_string(),
                }))
            },
        },
        SimpleExtensionDeclaration {
            mapping_type: {
                Some(MappingType::ExtensionFunction(ExtensionFunction {
                    extension_uri_reference: 2,
                    function_anchor: 5,
                    name: "gt".to_string(),
                }))
            },
        },
    ];

    match ast {
        Statement::Select(select_stmt) => {
            let plan_rel = select_statement_to_substrait(select_stmt)?;

            // Construct the Plan with the RelRoot
            let plan = Plan {
                relations: vec![plan_rel], // Add the RelRoot to the Plan
                extension_uris: extension_uris,
                extensions: extensions,
                ..Default::default()
            };
            Ok(plan) // Return the constructed Plan
        }
        Statement::CreateTable(create_stmt) => {
            let plan_rel = create_table_statement_to_substrait(create_stmt)?;
            let plan = Plan {
                relations: vec![plan_rel],
                ..Default::default()
            };
            Ok(plan)
        }
        // Handle other Statement variants
        _ => Err("Unsupported AST node type".into()),
    }
}
