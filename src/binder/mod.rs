use substrait::proto::{self, plan_rel::RelType, read_rel, Plan, PlanRel, Rel, RelRoot};

use crate::parser::ast::*;

fn select_statement_to_substrait(select: &SelectStatement) -> Result<PlanRel, Box<dyn std::error::Error>> {
    // Construct base schema
    //let base_schema = None;
    // NamedStruct {
    //     names: vec!["first_name".to_string(), "surname".to_string()],
    //     r#struct: Some(create_string_field(TypeNullability::NullabilityRequired)),
    // };

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


    // Construct the root relation
    let root_rel = RelRoot {
        names: construct_column_names(&select.columns), // Output column names
        input: Some(read_rel), // Input relation
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
                // Extend this match arm for other functions as needed
                // Add more cases for other aggregate functions
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