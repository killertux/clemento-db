use anyhow::Result;
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::future::Future;
use tokio::io::{stdin, AsyncBufReadExt, BufReader, Stdin};

#[tokio::main]
async fn main() -> Result<()> {
    let stdin = BufReader::new(stdin());
    let dialect = GenericDialect {};
    process_sql(stdin, &dialect, |ast| async {
        dbg!(ast);
        Ok(())
    })
    .await?;
    Ok(())
}

async fn process_sql<F, Fut>(
    stdin: BufReader<Stdin>,
    dialect: &GenericDialect,
    mut processor: F,
) -> Result<()>
where
    F: FnMut(Vec<Statement>) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let mut buffer = String::new();
    let mut lines = stdin.lines();
    while let Some(line) = lines.next_line().await? {
        match line.split_once(';') {
            None => {
                buffer += "\n";
                buffer += &line
            }
            Some((head, end)) => {
                buffer += "\n";
                buffer += head;
                processor(Parser::parse_sql(dialect, &buffer)?).await?;
                buffer = String::from(end);
            }
        }
    }
    Ok(())
}
