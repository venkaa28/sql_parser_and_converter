use std::fs::read;

use substrait::proto::{self, aggregate_rel::Grouping, expression::{self, field_reference::ReferenceType, reference_segment::StructField, FieldReference, ReferenceSegment, RexType, ScalarFunction}, function_argument::{self, ArgType}, plan_rel::RelType, read_rel, Expression, ExpressionReference, FunctionArgument, Plan, PlanRel, Rel, RelRoot};

use crate::parser::ast::*;

fn select_statement_to_substrait(select: &SelectStatement) -> Result<PlanRel, Box<dyn std::error::Error>> {

    // Construct read relation
    let read_rel = Rel {
        rel_type: Some(proto::rel::RelType::Read(Box::new(proto::ReadRel {
            read_type: Some(read_rel::ReadType::NamedTable(read_rel::NamedTable {
                names: vec![select.from.table.name.clone()], 
                advanced_extension: None,
            })),
            ..Default::default()
        }))),
        ..Default::default()
    };

    let mut input_rel = read_rel;

    if let Some(where_clause) = &select.where_clause {
        if let Condition::GreaterThan { column, value } = &where_clause.condition {
            let condition_expr = Expression {
                    rex_type: Some(expression::RexType::ScalarFunction(ScalarFunction {
                        function_reference: 1, //ref to greater than function
                        //add expressions to expression and function ref and correct ref here
                        arguments: vec![
                            FunctionArgument {
                                arg_type: Some(ArgType::Value(Expression {
                                    rex_type: Some(RexType::Selection(
                                        Box::new(FieldReference {
                                            reference_type: Some(ReferenceType::DirectReference(
                                                ReferenceSegment {
                                                    reference_type: Some(expression::reference_segment::ReferenceType::StructField(
                                                        Box::new(StructField {
                                                            field: 10,
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
                                        nullable: false, // Assuming the literal is not nullable
                                        type_variation_reference: 0, // Assuming default type variation
                                        literal_type: Some(expression::literal::LiteralType::I32(0)), // Literal value of 0
                                })),
                                })),
                            }
                        ],
                        ..Default::default()
                    })),
                    ..Default::default()
            };

        // Wrap the condition in a Filter operation

            input_rel = Rel {
                rel_type: Some(proto::rel::RelType::Filter(Box::new(proto::FilterRel {
                    input: Some(Box::new(input_rel)), // Use the read relation as input
                    condition: Some(Box::new(condition_expr)),
                    ..Default::default()
                }))),
                ..Default::default()
            };
        }
    }

    if let Some(group_by_columns) = &select.group_by{
        let grouping_columns = construct_column_names(&group_by_columns);
        let groupings = grouping_columns.into_iter().map(|name| {
            Expression {
                rex_type: Some(RexType::Selection(
                    Box::new(FieldReference {
                        reference_type: Some(ReferenceType::DirectReference(
                            ReferenceSegment {
                                reference_type: Some(expression::reference_segment::ReferenceType::StructField(
                                    Box::new(StructField {
                                        field: 10, //todo map field name to index of base schema
                                        child: None,
                                    })
                                )),
                            }
                        )),
                        root_type: None,
                    })
                ))
            }
        }).collect::<Vec<_>>();
    
        input_rel = Rel {
            rel_type: Some(proto::rel::RelType::Aggregate(Box::new(proto::AggregateRel {
                input: Some(Box::new(input_rel)), // Wrap the previous input_rel as the input to AggregateRel
                groupings: vec![Grouping {
                    grouping_expressions: groupings, // Use the constructed groupings
                }],
                ..Default::default()
            }))),
        };
    }

    if let Some(limit_clause) = &select.limit{
        input_rel = Rel {
            rel_type: Some(proto::rel::RelType::Fetch(Box::new(proto::FetchRel{
                input: Some(Box::new(input_rel)),
                count: *limit_clause as i64,
                ..Default::default()
            }))),
        };
    }

    // Construct the root relation
    let root_rel = RelRoot {
        names: construct_column_names(&select.columns), // Output column names
        input: Some(input_rel), // Input relation
        ..Default::default()
    };

    let rel_type = RelType::Root(root_rel);

    // Construct the plan with the root relation
    let plan_rel = PlanRel{
        rel_type: Some(rel_type)
    };

    Ok(plan_rel)
}

fn construct_column_names(columns: &[Column]) -> Vec<String> {
    columns.iter().map(|column| {
        match column {
            Column::Star => "*".to_string(),
            Column::Name(name) => name.clone(),
            Column::Function(func) => match func.func {
                AggregateFunction::Count => "count".to_string(),
                //figure out how to import the count extension from functions_aggregate_generic.yaml and reference the function in the root_rel names field
            },
            Column::Number(num) => num.to_string(),
        }
    }).collect()
}

pub fn ast_to_substrait_plan(ast: &Statement) -> Result<Plan, Box<dyn std::error::Error>> {
    match ast {
        Statement::Select(select_stmt) => {
            let plan_rel = select_statement_to_substrait(select_stmt)?;

            // Construct the Plan with the RelRoot
            let plan = Plan {
                relations: vec![plan_rel], // Add the RelRoot to the Plan
                ..Default::default()
            };
            Ok(plan) // Return the constructed Plan
        },
        // Handle other Statement variants
        _ => Err("Unsupported AST node type".into()),
    }
}