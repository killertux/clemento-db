use anyhow::Result;
use futures_util::{pin_mut, StreamExt};
use query_executor::{query_executor, LocalDiskTableStorage};
use sql_reader::process_sql;
use sqlparser::dialect::GenericDialect;
use tokio::io::stdin;

mod query_executor;
#[cfg(test)]
mod single_table_select_test;
mod sql_reader;

#[tokio::main]
async fn main() -> Result<()> {
    let dialect = GenericDialect {};
    let table_storage = LocalDiskTableStorage::new(";");
    let statements = process_sql(stdin(), &dialect).await;
    pin_mut!(statements);
    while let Some(statement) = statements.next().await {
        let statement = statement?;
        let result = query_executor(statement, &table_storage).await?;
        for line in result.into_iter() {
            println!("{}", line.join(";"));
        }
        println!();
    }
    Ok(())
}
