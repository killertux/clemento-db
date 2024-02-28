use anyhow::{anyhow, bail, Result};
use evaluate::evaluate_expression;
use query_plan::{select_plan, Fetch};
use sqlparser::ast::{Expr, Select, SelectItem, SetExpr, Statement};
pub use table_storage::*;

mod evaluate;
mod query_plan;
mod table_storage;

pub async fn query_executor(
    statement: Statement,
    table_storage: &impl TableStorage,
) -> Result<Vec<Vec<String>>> {
    match statement {
        Statement::Query(query) => match *query.body {
            SetExpr::Select(select) => {
                let execution_plan = select_plan(*select.clone())?;
                let (data_columns, data) = fetch_data(&execution_plan, table_storage).await?;
                let projections = handle_projections(*select, &data_columns)?;
                evaluate_expresions_and_return_result(
                    projections,
                    data,
                    data_columns,
                    &execution_plan.filters,
                )
            }
            _ => bail!("Unimplemented for {}", query.body),
        },
        _ => bail!("Unimplemented for {statement}"),
    }
}

fn evaluate_expresions_and_return_result(
    projections: Vec<(usize, String)>,
    data: Vec<Vec<String>>,
    data_columns: Vec<(String, String)>,
    filters: &Expr,
) -> Result<Vec<Vec<String>>> {
    let mut statement_result = Vec::new();
    statement_result.push(
        projections
            .iter()
            .map(|(_, alias)| alias.clone())
            .collect::<Vec<String>>(),
    );
    for row in data.into_iter() {
        if evaluate_expression(&row, &data_columns, filters)?.to_bool() {
            statement_result.push(
                projections
                    .iter()
                    .map(|(field, _)| {
                        row.get(*field)
                            .ok_or(anyhow!("Field {field} not found in data"))
                            .cloned()
                    })
                    .collect::<Result<Vec<String>>>()?,
            );
        }
    }
    Ok(statement_result)
}

fn handle_projections(
    select: Select,
    data_columns: &[(String, String)],
) -> Result<Vec<(usize, String)>> {
    let mut projections: Vec<(usize, String)> = Vec::new();
    for projection in select.projection {
        match projection {
            SelectItem::Wildcard(_) => {
                projections.extend(
                    data_columns
                        .iter()
                        .enumerate()
                        .map(|(index, (_, name))| (index, name.clone())),
                );
            }
            SelectItem::UnnamedExpr(expr) => {
                let Expr::Identifier(ident) = expr else {
                    bail!("Can only project identifiers. {expr} given")
                };
                let Some(pos) = data_columns
                    .iter()
                    .position(|(_, column)| *column == ident.value)
                else {
                    bail!("Column {} not found", ident.value)
                };
                projections.push((pos, ident.value));
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let Expr::Identifier(ident) = expr else {
                    bail!("Can only project identifiers. {expr} given")
                };
                let Some(pos) = data_columns
                    .iter()
                    .position(|(_, column)| *column == ident.value)
                else {
                    bail!("Column {} not found", ident.value)
                };
                projections.push((pos, alias.value));
            }
            _ => bail!("Unimplemented for {projection}"),
        }
    }
    Ok(projections)
}

async fn fetch_data(
    execution_plan: &query_plan::ExecutionPlan,
    table_storage: &impl TableStorage,
) -> Result<(Vec<(String, String)>, Vec<Vec<String>>), anyhow::Error> {
    let mut data_columns = Vec::new();
    let mut data = Vec::new();
    for table in &execution_plan.tables {
        let rows =
            match table.fetch {
                Fetch::FullScan => {
                    let mut table_reader = table_storage.read_table(&table.table).await?;
                    let header = table_reader.read_line().await?;
                    data_columns.extend(header.into_iter().map(|column| {
                        (table.alias.clone().unwrap_or(table.table.clone()), column)
                    }));
                    let mut rows: Vec<Vec<String>> = Vec::new();
                    while let Some(line) = table_reader.next_line().await? {
                        rows.push(line);
                    }
                    rows
                }
            };
        if data.is_empty() {
            data = rows;
        } else {
            data = data
                .into_iter()
                .flat_map(|exisent_row| {
                    rows.iter().map(move |new_row| {
                        exisent_row
                            .iter()
                            .chain(new_row.iter())
                            .map(|value| value.to_owned())
                            .collect::<Vec<String>>()
                    })
                })
                .collect();
        }
    }
    Ok((data_columns, data))
}
