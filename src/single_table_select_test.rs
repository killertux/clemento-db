use anyhow::{anyhow, bail, Result};
use futures_util::{pin_mut, StreamExt};
use sqlparser::dialect::GenericDialect;
use std::collections::HashMap;
use tokio::io::BufReader;

use crate::{process_sql, query_executor, AsyncBufReaderTableReader, TableStorage};

#[tokio::test]
async fn select_all() -> Result<()> {
    let table_storage = build_table_storage_for_test();
    let result = process_statement("SELECT * FROM test;", &table_storage).await?;
    assert_eq!(
        vec![
            vec!["id", "field1", "field2"],
            vec!["1", "First Name", "1001"],
            vec!["2", "Second Name", "2002"],
            vec!["3", "Third Name", "1001"]
        ],
        result
    );
    Ok(())
}

#[tokio::test]
async fn select_single_row() -> Result<()> {
    let table_storage = build_table_storage_for_test();
    let result = process_statement("SELECT * FROM test WHERE id = '1';", &table_storage).await?;
    assert_eq!(
        vec![
            vec!["id", "field1", "field2"],
            vec!["1", "First Name", "1001"],
        ],
        result
    );
    Ok(())
}

#[tokio::test]
async fn select_two_row() -> Result<()> {
    let table_storage = build_table_storage_for_test();
    let result =
        process_statement("SELECT * FROM test WHERE field2 = '1001';", &table_storage).await?;
    assert_eq!(
        vec![
            vec!["id", "field1", "field2"],
            vec!["1", "First Name", "1001"],
            vec!["3", "Third Name", "1001"],
        ],
        result
    );
    Ok(())
}

#[tokio::test]
async fn select_single_column() -> Result<()> {
    let table_storage = build_table_storage_for_test();
    let result = process_statement("SELECT id FROM test;", &table_storage).await?;
    assert_eq!(vec![vec!["id"], vec!["1"], vec!["2"], vec!["3"],], result);
    Ok(())
}

#[tokio::test]
async fn select_with_alias() -> Result<()> {
    let table_storage = build_table_storage_for_test();
    let result = process_statement(
        "SELECT id, field1 as name, field2 as account FROM test;",
        &table_storage,
    )
    .await?;
    assert_eq!(
        vec![
            vec!["id", "name", "account"],
            vec!["1", "First Name", "1001"],
            vec!["2", "Second Name", "2002"],
            vec!["3", "Third Name", "1001"]
        ],
        result
    );
    Ok(())
}

#[tokio::test]
async fn select_casting_field() -> Result<()> {
    let table_storage = build_table_storage_for_test();
    let result = process_statement(
        "SELECT * FROM test WHERE CAST(field2 as Integer) = 2002;",
        &table_storage,
    )
    .await?;
    assert_eq!(
        vec![
            vec!["id", "field1", "field2"],
            vec!["2", "Second Name", "2002"],
        ],
        result
    );
    Ok(())
}

#[tokio::test]
async fn select_casting_literal() -> Result<()> {
    let table_storage = build_table_storage_for_test();
    let result = process_statement(
        "SELECT * FROM test WHERE field2 = CAST(2002 as Text);",
        &table_storage,
    )
    .await?;
    assert_eq!(
        vec![
            vec!["id", "field1", "field2"],
            vec!["2", "Second Name", "2002"],
        ],
        result
    );
    Ok(())
}

#[tokio::test]
async fn select_and() -> Result<()> {
    let table_storage = build_table_storage_for_test();
    let result = process_statement(
        "SELECT * FROM test WHERE field1 = 'First Name' AND field2 = '1001';",
        &table_storage,
    )
    .await?;
    assert_eq!(
        vec![
            vec!["id", "field1", "field2"],
            vec!["1", "First Name", "1001"],
        ],
        result
    );
    Ok(())
}

#[tokio::test]
async fn select_or() -> Result<()> {
    let table_storage = build_table_storage_for_test();
    let result = process_statement(
        "SELECT * FROM test WHERE id = '1' OR id = '2';",
        &table_storage,
    )
    .await?;
    assert_eq!(
        vec![
            vec!["id", "field1", "field2"],
            vec!["1", "First Name", "1001"],
            vec!["2", "Second Name", "2002"],
        ],
        result
    );
    Ok(())
}

#[tokio::test]
async fn select_join() -> Result<()> {
    let table_storage = build_table_storage_for_test();
    let result = process_statement(
        "SELECT * FROM test JOIN test_2 ON test.id = test_2.test_id;",
        &table_storage,
    )
    .await?;
    assert_eq!(
        vec![
            vec!["id", "field1", "field2", "id", "test_id"],
            vec!["1", "First Name", "1001", "1", "1"],
            vec!["1", "First Name", "1001", "2", "1"],
            vec!["2", "Second Name", "2002", "3", "2"],
        ],
        result
    );
    Ok(())
}

#[tokio::test]
async fn select_join_with_alias() -> Result<()> {
    let table_storage = build_table_storage_for_test();
    let result = process_statement(
        "SELECT * FROM test t JOIN test_2 t2 ON t.id = t2.test_id;",
        &table_storage,
    )
    .await?;
    assert_eq!(
        vec![
            vec!["id", "field1", "field2", "id", "test_id"],
            vec!["1", "First Name", "1001", "1", "1"],
            vec!["1", "First Name", "1001", "2", "1"],
            vec!["2", "Second Name", "2002", "3", "2"],
        ],
        result
    );
    Ok(())
}

fn build_table_storage_for_test() -> InMemoryTableStorage {
    InMemoryTableStorage::new(";").add_table(
        "test",
        r#"id;field1;field2
1;First Name;1001
2;Second Name;2002
3;Third Name;1001
"#,
    ).add_table(
        "test_2",
        r#"id;test_id
1;1
2;1
3;2
"#,
    )
}

async fn process_statement(
    sql: impl AsRef<str>,
    table_storage: &impl TableStorage,
) -> Result<Vec<Vec<String>>> {
    let statements = process_sql(sql.as_ref().as_bytes(), &GenericDialect).await;
    pin_mut!(statements);
    let Some(statement) = statements.next().await else {
        bail!("Expect a statement")
    };
    let statement = statement?;
    Ok(query_executor(statement, table_storage).await?)
}

#[derive(Clone)]
struct InMemoryTableStorage {
    tables: HashMap<String, String>,
    separator: &'static str,
}

impl InMemoryTableStorage {
    pub fn new(separator: &'static str) -> Self {
        Self {
            separator: separator,
            tables: HashMap::new(),
        }
    }

    pub fn add_table(mut self, table: impl Into<String>, data: impl Into<String>) -> Self {
        self.tables.insert(table.into(), data.into());
        self
    }
}

impl TableStorage for InMemoryTableStorage {
    async fn read_table<'a>(&'a self, table: &str) -> Result<impl crate::TableReader + 'a> {
        Ok(AsyncBufReaderTableReader {
            separator: self.separator,
            buf_reader: BufReader::new(
                self.tables
                    .get(table)
                    .ok_or(anyhow!("Table not found {table}"))?
                    .as_bytes(),
            ),
        })
    }
}
