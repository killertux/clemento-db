use anyhow::{anyhow, bail, Result};
use async_stream::stream;
use bigdecimal::{BigDecimal, ToPrimitive};
use futures_util::{pin_mut, Stream, StreamExt};
use indexmap::IndexMap;
use sqlparser::ast::{
    BinaryOperator, DataType, Expr, JoinConstraint, JoinOperator, Select, SelectItem, SetExpr,
    Statement, TableFactor, Value as AstValue,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use tokio::fs::File;
use tokio::io::{stdin, AsyncBufRead, AsyncBufReadExt, AsyncRead, BufReader};

#[cfg(test)]
mod single_table_select_test;

#[tokio::main]
async fn main() -> Result<()> {
    let dialect = GenericDialect {};
    let table_storage = LocalDiskTableStorage::new("\t");
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

pub async fn process_sql<'a, R>(
    stdin: R,
    dialect: &'a GenericDialect,
) -> impl Stream<Item = Result<Statement>> + 'a
where
    R: AsyncRead + Unpin + 'a,
{
    let stdin = BufReader::new(stdin);
    let mut buffer = String::new();
    let mut lines = stdin.lines();
    stream! {
        while let Some(line) = lines.next_line().await? {
            match line.split_once(';') {
                None => {
                    buffer += "\n";
                    buffer += &line
                }
                Some((head, end)) => {
                    buffer += "\n";
                    buffer += head;
                    for statement in Parser::parse_sql(dialect, &buffer)? {
                        yield Ok(statement);
                    }
                    buffer = String::from(end);
                }
            }
        }
    }
}

pub async fn query_executor(
    statement: Statement,
    table_storage: &impl TableStorage,
) -> Result<Vec<Vec<String>>> {
    let mut statement_result = Vec::new();
    match statement {
        Statement::Query(query) => match *query.body {
            SetExpr::Select(select) => {
                let execution_plan = select_plan(*select.clone())?;
                let mut data = Vec::new();
                // let mut data_columns = IndexSet::new();
                for table in execution_plan.tables {
                    let rows = match table.fetch {
                        Fetch::FullScan => {
                            let mut table_reader = table_storage.read_table(&table.table).await?;
                            let header = table_reader.read_line().await?;
                            let mut rows: Vec<IndexMap<String, String>> = Vec::new();
                            while let Some(line) = table_reader.next_line().await? {
                                rows.push(header.iter().cloned().zip(line).collect());
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
                                        .map(|(index, value)| {
                                            (index.to_string(), value.to_string())
                                        })
                                        .collect::<IndexMap<String, String>>()
                                })
                            })
                            .collect();
                    }
                }
                let mut projections: Vec<(String, String)> = Vec::new();
                for projection in select.projection {
                    match projection {
                        SelectItem::Wildcard(_) => {
                            projections
                                .extend(data[0].keys().map(|key| (key.clone(), key.clone())));
                        }
                        SelectItem::UnnamedExpr(expr) => {
                            let Expr::Identifier(ident) = expr else {
                                bail!("Can only project identifiers. {expr} given")
                            };
                            if !data[0].contains_key(&ident.value) {
                                bail!("Column {} not found", ident.value)
                            }
                            projections.push((ident.value.clone(), ident.value));
                        }
                        SelectItem::ExprWithAlias { expr, alias } => {
                            let Expr::Identifier(ident) = expr else {
                                bail!("Can only project identifiers. {expr} given")
                            };
                            if !data[0].contains_key(&ident.value) {
                                bail!("Column {} not found", ident.value)
                            }
                            projections.push((ident.value, alias.value));
                        }
                        _ => bail!("Unimplemented for {projection}"),
                    }
                }
                statement_result.push(
                    projections
                        .iter()
                        .map(|(_, alias)| alias.clone())
                        .collect::<Vec<String>>(),
                );
                for row in data.into_iter() {
                    if evaluate_expression(&row, &execution_plan.filters)?.to_bool() {
                        statement_result.push(
                            projections
                                .iter()
                                .map(|(field, _)| {
                                    row.get(field)
                                        .ok_or(anyhow!("Field {field} not found in data"))
                                        .cloned()
                                })
                                .collect::<Result<Vec<String>>>()?,
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

fn evaluate_expression(row: &IndexMap<String, String>, expression: &Expr) -> Result<Value> {
    Ok(match expression {
        Expr::Identifier(identifier) => {
            let value = row
                .get(&identifier.value)
                .ok_or(anyhow!("Failed to find column {identifier}"))?;
            Value::String(value.clone())
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
        Expr::IsFalse(expr) => (!evaluate_expression(row, expr)?.to_bool()).into(),
        Expr::IsNotFalse(expr) => evaluate_expression(row, expr)?.to_bool().into(),
        Expr::IsTrue(expr) => evaluate_expression(row, expr)?.to_bool().into(),
        Expr::IsNotTrue(expr) => (!evaluate_expression(row, expr)?.to_bool()).into(),
        Expr::IsNull(expr) => matches!(evaluate_expression(row, expr)?, Value::Null).into(),
        Expr::IsNotNull(expr) => (!matches!(evaluate_expression(row, expr)?, Value::Null)).into(),
        Expr::BinaryOp { left, op, right } => {
            let left = evaluate_expression(row, left)?;
            match op {
                BinaryOperator::And => {
                    (left.to_bool() && evaluate_expression(row, right)?.to_bool()).into()
                }
                BinaryOperator::Or => {
                    (left.to_bool() || evaluate_expression(row, right)?.to_bool()).into()
                }
                BinaryOperator::Eq => (left == evaluate_expression(row, right)?).into(),
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
            let value = evaluate_expression(row, expr)?;
            match (&value, data_type) {
                (Value::Integer(value), DataType::Text) => Value::String(value.to_string()),
                (Value::String(value), DataType::Integer(_)) => Value::Integer(value.parse()?),
                _ => bail!("Cannot cast {value:?} into {data_type}"),
            }
        }
        _ => bail!("Not implemented expression {expression:?}"),
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

fn select_plan(mut select: Select) -> Result<ExecutionPlan> {
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
            alias: _,
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
        alias: _,
        args: _,
        with_hints: _,
        version: _,
        partitions: _,
    } = table.relation
    else {
        bail!("Can only process Tables. {} given.", table.relation)
    };
    execution_plan_tables.push(TableFetch {
        fetch: Fetch::FullScan,
        table: name.to_string(),
    });
    Ok(ExecutionPlan {
        tables: execution_plan_tables,
        filters,
    })
}

pub struct ExecutionPlan {
    tables: Vec<TableFetch>,
    filters: Expr,
}

pub struct TableFetch {
    fetch: Fetch,
    table: String,
}

pub enum Fetch {
    FullScan,
}

#[allow(async_fn_in_trait)]
pub trait TableStorage {
    async fn read_table<'a>(&'a self, table: &str) -> Result<impl TableReader + 'a>;
}

#[allow(async_fn_in_trait)]
pub trait TableReader {
    async fn read_line(&mut self) -> Result<Vec<String>>;
    async fn next_line(&mut self) -> Result<Option<Vec<String>>>;
}

struct LocalDiskTableStorage {
    separator: &'static str,
}

impl LocalDiskTableStorage {
    pub fn new(separator: &'static str) -> Self {
        Self { separator }
    }
}

struct AsyncBufReaderTableReader<R>
where
    R: AsyncBufRead + Unpin,
{
    buf_reader: R,
    separator: &'static str,
}

impl TableStorage for LocalDiskTableStorage {
    async fn read_table<'a>(&'a self, table: &str) -> Result<impl TableReader + 'a> {
        let file = File::open(format!("{}.tsv", table)).await?;
        let reader = BufReader::new(file);
        Ok(AsyncBufReaderTableReader {
            buf_reader: reader,
            separator: self.separator,
        })
    }
}

impl<R> TableReader for AsyncBufReaderTableReader<R>
where
    R: AsyncBufRead + Unpin,
{
    async fn read_line(&mut self) -> Result<Vec<String>> {
        let mut line = String::new();
        self.buf_reader.read_line(&mut line).await?;
        Ok(line
            .trim()
            .split(self.separator)
            .map(|value| value.into())
            .collect())
    }
    async fn next_line(&mut self) -> Result<Option<Vec<String>>> {
        let mut line = String::new();
        match self.buf_reader.read_line(&mut line).await {
            Ok(0) => Ok(None),
            Ok(_) => Ok(Some(
                line.trim()
                    .split(self.separator)
                    .map(|value| value.into())
                    .collect(),
            )),
            Err(err) => Err(err.into()),
        }
    }
}
