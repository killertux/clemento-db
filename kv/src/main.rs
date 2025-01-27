use std::net::SocketAddr;

use anyhow::{Context, Result};
use lsm::LSM;
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadHalf, WriteHalf},
    net::TcpStream,
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let (tx, rx) = tokio::sync::mpsc::channel(1024);
    let (node_1, tx_node_1) = Node::new(1, TcpNodeConnector::new("127.0.0.1:4242".parse()?), tx)?;
    Ok(())
}

struct Node {
    id: u64,
    join_handle_tx: JoinHandle<Result<()>>,
    join_handle_rx: JoinHandle<Result<()>>,
}

impl Node {
    pub fn new<C>(
        id: u64,
        connector: C,
        tx: Sender<()>,
    ) -> Result<(Self, Sender<TransmitterMessage<C::Writer>>)>
    where
        C: NodeConnector + Unpin + Send + 'static,
    {
        let (tx_transmitter, rx) = tokio::sync::mpsc::channel(1024);
        let join_handle_tx = tokio::spawn(Self::transmitter(rx));
        let tx_transmitter_clone = tx_transmitter.clone();
        let join_handle_rx = tokio::spawn(Self::receiver(id, connector, tx_transmitter_clone, tx));

        Ok((
            Self {
                id,
                join_handle_tx,
                join_handle_rx,
            },
            tx_transmitter,
        ))
    }

    async fn receiver<C>(
        id: u64,
        connector: C,
        mut transmitter: Sender<TransmitterMessage<C::Writer>>,
        mut tx: Sender<()>,
    ) -> Result<()>
    where
        C: NodeConnector,
    {
        loop {
            let (writer, mut reader) = connector.connect().await?;
            transmitter.send(TransmitterMessage::Stream(writer)).await?;

        }
        Ok(())
    }

    async fn transmitter<W>(mut rx: Receiver<TransmitterMessage<W>>) -> Result<()>
    where
        W: AsyncWrite + Unpin + Send,
    {
        let mut writer = None;
        while let Some(message) = rx.recv().await {
            match message {
                TransmitterMessage::Stream(stream) => {
                    writer = Some(stream);
                }
            }
        }
        Ok(())
    }
}

trait NodeConnector {
    type Writer: AsyncWrite + Unpin + Send + Sync + 'static;
    type Reader: AsyncRead + Unpin + Send;
    fn connect(
        &self,
    ) -> impl std::future::Future<Output = Result<(Self::Writer, Self::Reader)>> + Send;
}

struct TcpNodeConnector {
    addr: SocketAddr,
}

impl TcpNodeConnector {
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }
}

impl NodeConnector for TcpNodeConnector {
    type Writer = WriteHalf<TcpStream>;
    type Reader = ReadHalf<TcpStream>;
    async fn connect(&self) -> Result<(Self::Writer, Self::Reader)> {
        loop {
            match tokio::net::TcpStream::connect(&self.addr).await {
                Ok(stream) => {
                    let (reader, writer) = tokio::io::split(stream);
                    return Ok((writer, reader));
                }
                Err(e) => {
                    tracing::error!("Failed to connect to node: {}", e);
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }
    }
}

enum TransmitterMessage<Writer>
where
    Writer: AsyncWrite + Unpin + Send,
{
    Stream(Writer),
}
