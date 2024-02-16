use anyhow::{anyhow, bail, Result};
use bigdecimal::{BigDecimal, ToPrimitive};
use sqlparser::ast::{
    BinaryOperator, DataType, Expr, SelectItem, SetExpr, Statement, TableFactor, Value as AstValue,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::future::Future;
use tokio::fs::File;
use tokio::io::{stdin, AsyncBufReadExt, AsyncRead, BufReader};

#[tokio::main]
async fn main() -> Result<()> {
    let dialect = GenericDialect {};
    process_sql(stdin(), &dialect, |ast| async {
        for statement in ast.into_iter() {
            let result = query_executor(statement).await?;
            for line in result.into_iter() {
                println!("{}", line.join(";"));
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

async fn query_executor(statement: Statement) -> Result<Vec<Vec<String>>> {
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
                    if match &select.selection {
                        None => true,
                        Some(expr) => evaluate_expression(&header, &fields, expr)?.to_bool(),
                    } {
                        statement_result.push(
                            projections_indexes
                                .iter()
                                .map(|pos| fields[*pos].to_string())
                                .collect(),
                        );
                    }
                }
            }
            _ => bail!("Unimplemented for {}", query.body),
        },
        _ => bail!("Unimplemented for {statement}"),
    }
    Ok(statement_result)
}

fn evaluate_expression(header: &[&str], fields: &[&str], expression: &Expr) -> Result<Value> {
    Ok(match expression {
        Expr::Identifier(identifier) => {
            let pos = header
                .iter()
                .position(|column| *column == identifier.value)
                .ok_or(anyhow!("Failed to find column {identifier}"))?;
            Value::String(fields[pos].to_string())
        }
        Expr::Value(value) => match value {
            AstValue::Boolean(value) => (*value).into(),
            AstValue::DoubleQuotedString(string) | AstValue::SingleQuotedString(string) => {
                Value::String(string.clone())
            }
            AstValue::Null => Value::Null,
            AstValue::Number(decimal, _) => {
                if decimal.is_integer() {
                    Value::Integer(
                        decimal
                            .to_i64()
                            .ok_or(anyhow!("Number is not a valid integer"))?,
                    )
                } else {
                    Value::Decimal(decimal.clone())
                }
            }
            _ => bail!("Not implemented value {value}"),
        },
        Expr::IsFalse(expr) => (!evaluate_expression(header, fields, expr)?.to_bool()).into(),
        Expr::IsNotFalse(expr) => evaluate_expression(header, fields, expr)?.to_bool().into(),
        Expr::IsTrue(expr) => evaluate_expression(header, fields, expr)?.to_bool().into(),
        Expr::IsNotTrue(expr) => (!evaluate_expression(header, fields, expr)?.to_bool()).into(),
        Expr::IsNull(expr) => {
            matches!(evaluate_expression(header, fields, expr)?, Value::Null).into()
        }
        Expr::IsNotNull(expr) => {
            (!matches!(evaluate_expression(header, fields, expr)?, Value::Null)).into()
        }
        Expr::BinaryOp { left, op, right } => {
            let left = evaluate_expression(header, fields, left)?;
            match op {
                BinaryOperator::And => {
                    (left.to_bool() && evaluate_expression(header, fields, right)?.to_bool()).into()
                }
                BinaryOperator::Or => {
                    (left.to_bool() || evaluate_expression(header, fields, right)?.to_bool()).into()
                }
                BinaryOperator::Eq => (left == evaluate_expression(header, fields, right)?).into(),
                _ => bail!("Not implemented operator {op}"),
            }
        }
        Expr::Cast {
            expr,
            data_type,
            format,
        } => {
            if format.is_some() {
                bail!("Cannot format in casts");
            }
            let value = evaluate_expression(header, fields, expr)?;
            match (&value, data_type) {
                (Value::Integer(value), DataType::Text) => Value::String(value.to_string()),
                _ => bail!("Cannot cast {value:?} into {data_type}"),
            }
        }
        _ => bail!("Not implemented expresion {expression}"),
    })
}

#[derive(Debug)]
enum Value {
    Bool(bool),
    Integer(i64),
    Decimal(BigDecimal),
    String(String),
    Null,
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Bool(l), Self::Bool(r)) => l == r,
            (Self::Integer(l), Self::Integer(r)) => l == r,
            (Self::String(l), Self::String(r)) => l == r,
            (Self::Decimal(l), Self::Decimal(r)) => l == r,
            (Self::Null, _) | (_, Self::Null) => false,
            _ => false,
        }
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Value::Bool(value)
    }
}

impl Value {
    pub fn to_bool(&self) -> bool {
        match self {
            Value::Bool(value) => *value,
            Value::Integer(integer) => *integer != 0,
            Value::String(string) => !string.is_empty(),
            Value::Decimal(decimal) => decimal.to_i64().map(|n| n != 0).unwrap_or(true),
            Value::Null => false,
        }
    }
}
