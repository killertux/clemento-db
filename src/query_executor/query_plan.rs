use anyhow::{bail, Result};
use sqlparser::ast::{
    BinaryOperator, Expr, JoinConstraint, JoinOperator, Select, TableFactor, Value as AstValue,
};

pub fn select_plan(mut select: Select) -> Result<ExecutionPlan> {
    if select.from.len() != 1 {
        bail!("Can only process selects with one table");
    }
    let table = select.from.remove(0);
    let mut execution_plan_tables = Vec::new();
    let mut filters = select
        .selection
        .unwrap_or(Expr::Value(AstValue::Boolean(true)));
    for join in table.joins {
        let TableFactor::Table {
            name,
            alias,
            args: _,
            with_hints: _,
            version: _,
            partitions: _,
        } = join.relation
        else {
            bail!("Can only process Tables. {} given.", table.relation)
        };
        execution_plan_tables.push(TableFetch {
            fetch: Fetch::FullScan,
            table: name.to_string(),
            alias: alias.map(|alias| alias.name.to_string()),
        });
        let JoinOperator::Inner(constraint) = join.join_operator else {
            bail!("Cannot process join {:?}", join.join_operator)
        };
        match constraint {
            JoinConstraint::None => {}
            JoinConstraint::On(expr) => {
                filters = Expr::BinaryOp {
                    left: Box::new(filters),
                    right: Box::new(expr),
                    op: BinaryOperator::And,
                }
            }
            _ => bail!("Unimplemented join constraint {constraint:?}"),
        }
    }
    let TableFactor::Table {
        name,
        alias,
        args: _,
        with_hints: _,
        version: _,
        partitions: _,
    } = table.relation
    else {
        bail!("Can only process Tables. {} given.", table.relation)
    };
    execution_plan_tables.insert(
        0,
        TableFetch {
            fetch: Fetch::FullScan,
            table: name.to_string(),
            alias: alias.map(|alias| alias.name.to_string()),
        },
    );
    Ok(ExecutionPlan {
        tables: execution_plan_tables,
        filters,
    })
}

pub struct ExecutionPlan {
    pub tables: Vec<TableFetch>,
    pub filters: Expr,
}

pub struct TableFetch {
    pub fetch: Fetch,
    pub table: String,
    pub alias: Option<String>,
}

pub enum Fetch {
    FullScan,
}
