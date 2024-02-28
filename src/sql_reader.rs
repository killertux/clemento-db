use anyhow::Result;
use async_stream::stream;
use futures_util::Stream;
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};

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
            buffer += &line;
            buffer += "\n";
            while let Some((head, end)) = buffer.split_once(';') {
                for statement in Parser::parse_sql(dialect, head)? {
                    yield Ok(statement);
                }
                buffer = end.into();
            }
        }
    }
}
