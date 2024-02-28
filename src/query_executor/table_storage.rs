use anyhow::Result;
use tokio::fs::File;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, BufReader};

#[allow(async_fn_in_trait)]
pub trait TableStorage {
    async fn read_table<'a>(&'a self, table: &str) -> Result<impl TableReader + 'a>;
}

#[allow(async_fn_in_trait)]
pub trait TableReader {
    async fn read_line(&mut self) -> Result<Vec<String>>;
    async fn next_line(&mut self) -> Result<Option<Vec<String>>>;
}

pub struct LocalDiskTableStorage {
    separator: &'static str,
}

impl LocalDiskTableStorage {
    pub fn new(separator: &'static str) -> Self {
        Self { separator }
    }
}

pub struct AsyncBufReaderTableReader<R>
where
    R: AsyncBufRead + Unpin,
{
    buf_reader: R,
    separator: &'static str,
}

impl<R> AsyncBufReaderTableReader<R>
where
    R: AsyncBufRead + Unpin,
{
    pub fn new(buf_reader: R, separator: &'static str) -> Self {
        Self {
            buf_reader,
            separator,
        }
    }
}

impl TableStorage for LocalDiskTableStorage {
    async fn read_table<'a>(&'a self, table: &str) -> Result<impl TableReader + 'a> {
        let file = File::open(format!("{}.tsv", table)).await?;
        let reader = BufReader::new(file);
        Ok(AsyncBufReaderTableReader::new(reader, self.separator))
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
