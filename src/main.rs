use anyhow::{anyhow, bail, Result};
use sqlparser::ast::{Expr, SelectItem, SetExpr, Statement, TableFactor};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::future::Future;
use tokio::fs::File;
use tokio::io::{stdin, AsyncBufReadExt, AsyncRead, BufReader};
#[tokio::main]
async fn main() -> Result<()> {
    let dialect = GenericDialect {};
    process_sql(stdin(), &dialect, |ast| async {
        let result = query_executor(ast).await?;
        for statement_result in result.into_iter() {
            for line in statement_result.into_iter() {
                println!("{}", line.join("\t"));
            }
            println!();
        }
        Ok(())
    })
    .await?;

    Ok(())
}

async fn process_sql<R, F, Fut>(stdin: R, dialect: &GenericDialect, mut processor: F) -> Result<()>
where
    R: AsyncRead + Unpin,
    F: FnMut(Vec<Statement>) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let stdin = BufReader::new(stdin);
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

async fn query_executor(statements: Vec<Statement>) -> Result<Vec<Vec<Vec<String>>>> {
    let mut result = Vec::new();
    for statement in statements {
        let mut statement_result = Vec::new();
        match statement {
            Statement::Query(query) => match *query.body {
                SetExpr::Select(mut select) => {
                    if select.from.len() != 1 {
                        bail!("Can only process selects with one table");
                    }
                    let table = select.from.remove(0);
                    if !table.joins.is_empty() {
                        bail!("Cannot process joins");
                    }
                    let TableFactor::Table {
                        name,
                        alias: _,
                        args: _,
                        with_hints: _,
                        version: _,
                        partitions: _,
                    } = table.relation
                    else {
                        bail!("Can only process Tables. {} given.", table.relation)
                    };
                    let file = File::open(format!("{name}.tsv")).await?;
                    let mut reader = BufReader::new(file);
                    let mut header = String::new();
                    reader.read_line(&mut header).await?;
                    let header: Vec<&str> = header.trim().split('\t').collect();
                    let mut projections_indexes: Vec<usize> = Vec::new();
                    let mut header_result = Vec::new();
                    for projection in select.projection {
                        match projection {
                            SelectItem::Wildcard(_) => {
                                projections_indexes.append(&mut (0..header.len()).collect());
                                header_result.append(
                                    &mut projections_indexes
                                        .iter()
                                        .map(|pos| header[*pos].to_string())
                                        .collect(),
                                );
                            }
                            SelectItem::UnnamedExpr(expr) => {
                                let Expr::Identifier(ident) = expr else {
                                    bail!("Can only project identifiers. {expr} given")
                                };
                                let pos = header
                                    .iter()
                                    .position(|column| *column == ident.value)
                                    .ok_or(anyhow!("Cannot find column {ident}"))?;
                                projections_indexes.push(pos);
                                header_result.push(ident.value);
                            }
                            SelectItem::ExprWithAlias { expr, alias } => {
                                let Expr::Identifier(ident) = expr else {
                                    bail!("Can only project identifiers. {expr} given")
                                };
                                let pos = header
                                    .iter()
                                    .position(|column| *column == ident.value)
                                    .ok_or(anyhow!("Cannot find column {ident}"))?;
                                projections_indexes.push(pos);
                                header_result.push(alias.value);
                            }
                            _ => bail!("Unimplemented for {projection}"),
                        }
                    }
                    statement_result.push(header_result);
                    let mut lines = reader.lines();
                    while let Some(line) = lines.next_line().await? {
                        let fields: Vec<&str> = line.trim().split('\t').collect();
                        statement_result.push(
                            projections_indexes
                                .iter()
                                .map(|pos| fields[*pos].to_string())
                                .collect(),
                        );
                    }
                }
                _ => bail!("Unimplemented for {}", query.body),
            },
            _ => bail!("Unimplemented for {statement}"),
        }
        result.push(statement_result);
    }
    Ok(result)
}
