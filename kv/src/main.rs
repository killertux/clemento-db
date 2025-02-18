use std::collections::HashSet;
use std::{
    collections::HashMap, env::args, fmt::Formatter, net::SocketAddr, sync::Arc, time::Duration,
};

use anyhow::{anyhow, Result};
use bincode::de::Decoder;
use bincode::enc::write::Writer;
use bincode::enc::Encoder;
use bincode::error::{DecodeError, EncodeError};
use bincode::{config::Configuration, decode_from_slice, encode_to_vec, Decode, Encode};
use bytes::Bytes;
use lsm::LSM;
use rand::Rng;
use tokio::signal;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpListener, TcpStream,
    },
    select,
    sync::{
        mpsc::{Receiver, Sender},
        Mutex,
    },
    time::sleep,
};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_file(true)
        .with_line_number(true)
        .init();
    let token = CancellationToken::new();
    let nodes = [
        NodeIdentifier {
            id: 1,
            addr: "127.0.0.1:4242".parse()?,
        },
        NodeIdentifier {
            id: 2,
            addr: "127.0.0.1:4243".parse()?,
        },
        NodeIdentifier {
            id: 3,
            addr: "127.0.0.1:4244".parse()?,
        },
    ];
    let node_id: u64 = args().nth(1).expect("Missing node id").parse()?;
    let node = Node::<TcpNodeStream>::new(token.clone(), &nodes, node_id).await?;
    select! {
        _ = node.run() => {},
        _ = signal::ctrl_c() => {
            token.cancel();
        }
    }
    Ok(())
}

#[derive(Clone)]
struct Node<S>
where
    S: NodeStream + Clone,
{
    inner: Arc<InnerNode<S>>,
    inner_mutable: Arc<Mutex<InnerMutableNode>>,
}

struct InnerNode<S> {
    node_id: u64,
    cancellation_token: CancellationToken,
    nodes: Arc<HashMap<u64, NodeConnection<S>>>,
    config: Configuration,
}

struct InnerMutableNode {
    commit_index: u64,
    last_applied: u64,
    persistent_state: PersistentState,
    state: NodeState,
    node_id: u64,
    data: HashMap<String, Bytes>,
}

struct PersistentState {
    config: Configuration,
    current_term: u32,
    voted_for: Option<u64>,
    lsm: LSM,
}

impl PersistentState {
    pub async fn new(lsm: LSM, config: Configuration) -> Result<Self> {
        let state: PersistentStateValue = lsm
            .get(&encode_to_vec(&LsmEntryKey::PersistentState, config)?.into())
            .await?
            .map(|state| decode_from_slice(&state, config))
            .transpose()?
            .unwrap_or_default()
            .0;
        Ok(Self {
            config,
            current_term: state.current_term,
            voted_for: state.voted_for,
            lsm,
        })
    }

    pub fn current_term(&self) -> u32 {
        self.current_term
    }

    pub fn voted_for(&self) -> Option<u64> {
        self.voted_for
    }

    pub async fn last_log(&self) -> Result<LastLogValue> {
        Ok(self
            .lsm
            .get(&encode_to_vec(&LsmEntryKey::LastLog, self.config)?.into())
            .await?
            .map(|last_log_index| {
                decode_from_slice::<LastLogValue, _>(&last_log_index, self.config)
            })
            .transpose()?
            .unwrap_or_default()
            .0)
    }

    pub async fn set_last_log(&mut self, last_log: LastLogValue) -> Result<()> {
        self.lsm
            .put(
                std::borrow::Cow::Owned(encode_to_vec(&LsmEntryKey::LastLog, self.config)?.into()),
                encode_to_vec(last_log, self.config)?.into(),
            )
            .await?;
        Ok(())
    }

    pub async fn set_persistent_state_value(&mut self, state: PersistentStateValue) -> Result<()> {
        self.current_term = state.current_term;
        self.voted_for = state.voted_for;
        self.lsm
            .put(
                std::borrow::Cow::Owned(
                    encode_to_vec(&LsmEntryKey::PersistentState, self.config)?.into(),
                ),
                encode_to_vec(state, self.config)?.into(),
            )
            .await?;
        Ok(())
    }

    pub async fn get_entry(&self, entry_id: u64) -> Result<Option<Entry>> {
        Ok(self
            .lsm
            .get(&encode_to_vec(&LsmEntryKey::Entry(entry_id), self.config)?.into())
            .await?
            .map(|bytes| decode_from_slice::<Entry, _>(&bytes, self.config))
            .transpose()?
            .map(|res| res.0))
    }

    pub async fn set_entry(&mut self, entry: Entry) -> Result<()> {
        self.lsm
            .put(
                std::borrow::Cow::Owned(
                    encode_to_vec(&LsmEntryKey::Entry(entry.index), self.config)?.into(),
                ),
                encode_to_vec(&entry, self.config)?.into(),
            )
            .await?;
        Ok(())
    }
}

struct NodeConnection<S> {
    stream: S,
    sender: Sender<Message>,
    receiver: Mutex<Option<Receiver<Message>>>,
    join_handle_sender: Mutex<Option<tokio::task::JoinHandle<()>>>,
    join_handle_receiver: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

struct SenderWithJoinHandle<M> {
    sender: Sender<M>,
    join_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

struct ElectionTimeReset;

enum NodeState {
    Follower {
        leader_id: Option<u64>,
    },
    Candidate {
        votes: HashSet<u64>,
    },
    Leader {
        next_index: HashMap<u64, u64>,
        match_index: HashMap<u64, u64>,
        broadcast: Sender<Message>,
    },
}

impl<S> Node<S>
where
    S: NodeStream + Clone + Send + Sync + 'static + std::fmt::Debug,
{
    pub async fn new(
        token: CancellationToken,
        nodes_indentifiers: &[NodeIdentifier],
        node_id: u64,
    ) -> Result<Self> {
        let mut nodes = HashMap::new();
        let config = Configuration::default();
        for identifier in nodes_indentifiers {
            if identifier.id == node_id {
                continue;
            }
            let (sender_send, receiver_send) = tokio::sync::mpsc::channel(1024);
            let (sender_recv, receiver_recv) = tokio::sync::mpsc::channel(1024);
            let stream = S::new(identifier.clone(), node_id);
            let join_handle_sender = tokio::spawn(Self::sender_loop(
                token.clone(),
                stream.clone(),
                receiver_send,
            ));
            let join_handle_receiver = tokio::spawn(Self::receiver_loop(
                token.clone(),
                stream.clone(),
                sender_recv,
            ));
            nodes.insert(
                identifier.id,
                NodeConnection {
                    stream,
                    sender: sender_send,
                    receiver: Mutex::new(Some(receiver_recv)),
                    join_handle_sender: Mutex::new(Some(join_handle_sender)),
                    join_handle_receiver: Mutex::new(Some(join_handle_receiver)),
                },
            );
        }
        let nodes = Arc::new(nodes);
        for identifier in nodes_indentifiers {
            if identifier.id == node_id {
                let nodes = nodes.clone();
                let stream = S::new(identifier.clone(), node_id);
                tokio::spawn(async move {
                    stream
                        .handle_new(move |id| {
                            nodes
                                .get(&id)
                                .expect(&format!("Unexpected not found id {}", id))
                                .stream
                                .clone()
                        })
                        .await
                });
            }
        }
        let lsm = LSM::new(
            1024 * 1024 * 64,
            4,
            format!("./data/node_{node_id}",),
            false,
        )
        .await?;

        let inner = Arc::new(InnerNode {
            node_id,
            cancellation_token: token.clone(),
            nodes,
            config,
        });

        let inner_mutable = Arc::new(Mutex::new(InnerMutableNode {
            commit_index: 0,
            last_applied: 0,
            persistent_state: PersistentState::new(lsm, config.clone()).await?,
            node_id,
            state: NodeState::Follower { leader_id: None },
            data: HashMap::new(),
        }));

        tracing::info!("Node {} started", node_id);
        Ok(Self {
            inner,
            inner_mutable,
        })
    }

    pub async fn run(self) -> Result<()> {
        let (election_sender, election_receiver) = tokio::sync::mpsc::channel(1024);
        let (tx, mut rx) = tokio::sync::mpsc::channel(1024);
        let election_task = tokio::spawn(Self::election_loop(self.clone(), election_receiver));

        let mut join_set = JoinSet::new();
        for (node_id, connection) in self.inner.nodes.iter() {
            join_set.spawn(Self::node_reader(
                tx.clone(),
                *node_id,
                connection
                    .receiver
                    .lock()
                    .await
                    .take()
                    .ok_or(anyhow!("No receiver found for node_id {node_id}"))?,
                self.inner.cancellation_token.clone(),
            ));
        }

        loop {
            select! {
                _ = self.inner.cancellation_token.cancelled() => break,
                msg = rx.recv() => {
                    let Some((node_id, msg)) = msg else {
                        continue;
                    };
                    match msg {
                        Message::ClientId(client_id) => {
                            tracing::info!("Received client id {}", client_id);
                        }
                        Message::RequestVote { term, candidate_id, last_log_index, last_log_term } => {
                            election_sender.send(ElectionTimeReset).await?;
                            let mut this = self.inner_mutable.lock().await;
                            let sender = &self.inner.nodes.get(&node_id).ok_or(anyhow!("Erro getting node sender {node_id}"))?.sender;

                            if term < this.persistent_state.current_term() {
                                sender.send(Message::RequestVoteResponse {
                                    term: this.persistent_state.current_term(),
                                    vote_granted: false,
                                }).await?;
                                continue;
                            }
                            if this.persistent_state.voted_for().map(|voted_for_id| voted_for_id != candidate_id).unwrap_or(false) {
                                sender.send(Message::RequestVoteResponse {
                                    term: this.persistent_state.current_term(),
                                    vote_granted: false,
                                }).await?;
                                continue;
                            }
                            let last_log = this.persistent_state.last_log().await?;
                            if last_log.term > last_log_term || last_log.index > last_log_index {
                                sender.send(Message::RequestVoteResponse {
                                    term: this.persistent_state.current_term(),
                                    vote_granted: false,
                                }).await?;
                                continue;
                            }
                            this.persistent_state.set_persistent_state_value(PersistentStateValue{
                                current_term: term,
                                voted_for: Some(candidate_id),
                            }).await?;
                            sender.send(Message::RequestVoteResponse {
                                term: this.persistent_state.current_term(),
                                vote_granted: true,
                            }).await?;
                        }
                        Message::RequestVoteResponse { term, vote_granted } => {
                            let mut this = self.inner_mutable.lock().await;
                            if this.persistent_state.current_term() < term {
                                this.persistent_state.set_persistent_state_value(PersistentStateValue{
                                    current_term: term,
                                    voted_for: None,
                                }).await?;
                                this.state = NodeState::Follower {
                                    leader_id: None,
                                };
                                continue;
                            }
                            if this.persistent_state.current_term() > term {
                                continue;
                            }
                            if vote_granted {
                                match &mut this.state {
                                    NodeState::Candidate { ref mut votes } => {
                                        votes.insert(node_id);
                                        let len = votes.len();
                                        this.state = NodeState::Candidate {
                                            votes: votes.clone(),
                                        };
                                        if len > self.inner.nodes.len() / 2 {
                                            tracing::info!("Node {} is now leader", this.node_id);
                                            let (tx, rx) = tokio::sync::mpsc::channel(1024);
                                            tokio::spawn(Self::broadcast(rx, this.persistent_state.current_term(), self.inner.clone(), self.inner_mutable.clone(), self.inner.cancellation_token.clone()));
                                            this.state = NodeState::Leader {
                                                next_index: self.inner.nodes.iter().map(|(id, _)| (*id, 0)).collect(),
                                                match_index: self.inner.nodes.iter().map(|(id, _)| (*id, 0)).collect(),
                                                broadcast: tx,
                                            };
                                        }

                                    }
                                    _ => {}
                                }
                            }
                        }
                        Message::AppendEntries { term, leader_id, prev_log_index, prev_log_term, entries, leader_commit } => {
                            let sender = &self.inner.nodes.get(&node_id).ok_or(anyhow!("Erro getting node sender {node_id}"))?.sender;
                            election_sender.send(ElectionTimeReset).await?;
                            let mut this = self.inner_mutable.lock().await;
                            if term < this.persistent_state.current_term() {
                                sender.send(Message::AppendEntriesResponse {
                                    term: this.persistent_state.current_term(),
                                    success: false,
                                }).await?;
                                continue;
                            }
                            if term > this.persistent_state.current_term() {
                                this.persistent_state.set_persistent_state_value(PersistentStateValue{
                                    current_term: term,
                                    voted_for: None,
                                }).await?;
                                this.state = NodeState::Follower {
                                    leader_id: Some(leader_id),
                                };
                            }
                            if term == this.persistent_state.current_term() && !matches!(this.state, NodeState::Follower { .. }) {
                                this.persistent_state.set_persistent_state_value(PersistentStateValue{
                                    current_term: term,
                                    voted_for: None,
                                }).await?;
                                this.state = NodeState::Follower {
                                    leader_id: Some(leader_id),
                                };
                            }
                            let last_log = this.persistent_state.last_log().await?;
                            if last_log.index != prev_log_index || last_log.term != prev_log_term {
                                sender.send(Message::AppendEntriesResponse {
                                    term: this.persistent_state.current_term(),
                                    success: false,
                                }).await?;
                                continue;
                            }
                            let mut last_log_index = last_log.index;
                            let mut last_log_term = last_log.term;
                            for entry in entries {
                                last_log_index = entry.index;
                                last_log_term = entry.term;
                                this.persistent_state.set_entry(entry).await?;
                            }
                            if last_log_index != last_log.index || last_log_term != last_log.term {
                                this.persistent_state.set_last_log(LastLogValue{
                                    index: last_log_index,
                                    term: last_log_term,
                                }).await?;
                            }
                            if leader_commit > this.commit_index {
                                let last_commit_index = this.commit_index;
                                this.commit_index = leader_commit.min(last_log_index);
                                for index in last_commit_index..this.commit_index {
                                    let entry = this.persistent_state.get_entry(index).await?.ok_or(anyhow!("Entry not found"))?;
                                    match entry.operation {
                                        Operation::Set { key, value } => {
                                            this.data.insert(key, value.into());
                                        }
                                        Operation::Delete { key } => {
                                            this.data.remove(&key);
                                        }
                                        Operation::Get { .. } => {}
                                    }
                                }
                            }
                            sender.send(Message::AppendEntriesResponse {
                                term: this.persistent_state.current_term(),
                                success: true,
                            }).await?;
                        }
                        Message::AppendEntriesResponse { term, success } => {
                            election_sender.send(ElectionTimeReset).await?;
                            let mut this = self.inner_mutable.lock().await;
                            if term < this.persistent_state.current_term() {
                                continue;
                            }
                            if term > this.persistent_state.current_term() {
                                this.persistent_state.set_persistent_state_value(PersistentStateValue{
                                    current_term: term,
                                    voted_for: None,
                                }).await?;
                                this.state = NodeState::Follower {
                                    leader_id: None,
                                };
                            }
                        }
                    }
                }
            };
        }
        election_task.await??;
        for node in self.inner.nodes.iter() {
            let join_handle_sender = node.1.join_handle_sender.lock().await.take();
            if let Some(join_handle_sender) = join_handle_sender {
                join_handle_sender.await?;
            }
            let join_handle_receiver = node.1.join_handle_receiver.lock().await.take();
            if let Some(join_handle_receiver) = join_handle_receiver {
                join_handle_receiver.await?;
            }
        }
        Ok(())
    }

    async fn broadcast(
        mut rx: Receiver<Message>,
        term: u32,
        inner: Arc<InnerNode<S>>,
        inner_muttable: Arc<Mutex<InnerMutableNode>>,
        token: CancellationToken,
    ) {
        let await_time = Duration::from_millis(50);
        loop {
            let start = Instant::now();
            select! {
                _ = token.cancelled() => {
                    break;
                },
                _ = tokio::time::sleep(await_time) => {
                    let inner_mut = inner_muttable.lock().await;
                    if !matches!(inner_mut.state, NodeState::Leader { .. }) {
                        break;
                    }
                    if inner_mut.persistent_state.current_term() != term {
                        break;
                    }
                    if Instant::now() - start < await_time {
                        continue;
                    }
                    let msg = Message::AppendEntries {
                        term,
                        leader_id: inner.node_id,
                        prev_log_index: 0,
                        prev_log_term: 0,
                        entries: vec![],
                        leader_commit: 0,
                    };
                    for node in inner.nodes.iter() {
                        let _ = node.1.sender.send(msg.clone()).await;
                    }
                }
                msg = rx.recv() => {
                    let msg = msg.expect("Unexpected error");
                    for node in inner.nodes.iter() {
                        let _ = node.1.sender.send(msg.clone()).await;
                    }
                }
            }
        }
    }

    async fn node_reader(
        tx: Sender<(u64, Message)>,
        node_id: u64,
        mut rx: Receiver<Message>,
        token: CancellationToken,
    ) {
        loop {
            select! {
                _ = token.cancelled() => {
                    break;
                },
                msg = rx.recv() => {
                    let msg = msg.expect("Unexpected error");
                    let _ = tx.send((node_id, msg)).await;
                }
            }
        }
    }

    async fn sender_loop(
        token: CancellationToken,
        node_stream: S,
        mut receiver: Receiver<Message>,
    ) {
        loop {
            select! {
                msg = receiver.recv() => {
                    if msg.is_none() {
                        continue;
                    }
                    node_stream.send(msg.unwrap()).await;
                },
                _ = token.cancelled() => {
                    break;
                }
            }
        }
    }

    async fn receiver_loop(token: CancellationToken, node_stream: S, sender: Sender<Message>) {
        loop {
            select! {
                _ = token.cancelled() => {
                    break;
                },
                msg = node_stream.recv() => {
                    tracing::debug!("Received message {:?}", msg);
                    let _ = sender.send(msg).await;
                }
            }
        }
    }

    async fn election_loop(self, mut receiver: Receiver<ElectionTimeReset>) -> Result<()> {
        loop {
            let start = Instant::now();
            let await_time = rand::rng().random_range(150..300);
            let await_time = Duration::from_millis(await_time);
            select! {
                _ = self.inner.cancellation_token.cancelled() => {
                    break;
                },
                _ = tokio::time::sleep(await_time) => {
                    if Instant::now() - start < await_time {
                        continue;
                    }
                    let mut this = self.inner_mutable.lock().await;
                    if matches!(this.state, NodeState::Follower{ .. } | NodeState::Candidate { .. } ) {
                        let new_term = this.persistent_state.current_term() + 1;
                        let voted_for = Some(this.node_id);
                        this.persistent_state.set_persistent_state_value(PersistentStateValue{
                            current_term: new_term,
                            voted_for,
                        }).await?;
                        this.state = NodeState::Candidate {
                            votes: HashSet::from([this.node_id]),
                        };
                        let last_log = this.persistent_state.last_log().await?;
                        for node in self.inner.nodes.iter() {
                            let _ = node.1.sender.send(Message::RequestVote {
                                term: new_term,
                                candidate_id: this.node_id,
                                last_log_index: last_log.index,
                                last_log_term: last_log.term,
                            }).await;
                        }
                    }
                },
                _ = receiver.recv() => {
                    continue;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Encode, Decode)]
enum LsmEntryKey {
    PersistentState,
    LastLog,
    Entry(u64),
}

#[derive(Debug, Clone, Encode, Decode, Default)]
struct PersistentStateValue {
    current_term: u32,
    voted_for: Option<u64>,
}

#[derive(Debug, Clone, Encode, Decode, Default)]
struct LastLogValue {
    index: u64,
    term: u32,
}

#[derive(Debug, Clone)]
struct NodeIdentifier {
    id: u64,
    addr: SocketAddr,
}

trait NodeStream {
    type Stream;
    fn new(identifier: NodeIdentifier, current_node_id: u64) -> Self;
    fn send(&self, msg: Message) -> impl std::future::Future<Output = ()> + Send;
    fn recv(&self) -> impl std::future::Future<Output = Message> + Send;
    fn handle_new<F, N>(&self, get_node_stream: F) -> impl std::future::Future<Output = ()> + Send
    where
        F: Fn(u64) -> N + Send,
        N: NodeStream<Stream = Self::Stream> + Send;
    fn push_stream(&self, stream: Self::Stream) -> impl std::future::Future<Output = ()> + Send;
}

#[derive(Clone)]
struct TcpNodeStream {
    identifier: NodeIdentifier,
    current_node_id: u64,
    config: Configuration,
    read_stream: Arc<Mutex<Option<BufReader<OwnedReadHalf>>>>,
    write_stream: Arc<Mutex<Option<BufWriter<OwnedWriteHalf>>>>,
    external_stream: Arc<Mutex<Option<TcpStream>>>,
}

impl std::fmt::Debug for TcpNodeStream {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpNodeStream")
            .field("identifier", &self.identifier)
            .field("current_node_id", &self.current_node_id)
            .finish()
    }
}

impl TcpNodeStream {
    async fn reconnect(&self) {
        // Clear streams
        {
            let _ = self.read_stream.lock().await.take();
            let _ = self.write_stream.lock().await.take();
        }
        if self.current_node_id > self.identifier.id {
            self.await_for_external_tcp_stream().await;
            return;
        }
        loop {
            tracing::info!(
                "Trying to connect to node {} at address {}",
                self.identifier.id,
                self.identifier.addr
            );
            match TcpStream::connect(self.identifier.addr).await {
                Ok(stream) => match self.internal_handle_new_connection(stream).await {
                    Ok(_) => {
                        tracing::info!("Connected to node {}", self.identifier.id);
                        break;
                    }
                    Err(err) => {
                        tracing::error!("Error connecting to node {}: {}", self.identifier.id, err);
                        sleep(Duration::from_millis(300)).await;
                    }
                },
                Err(err) => {
                    tracing::error!("Error connecting to node {}: {}", self.identifier.id, err);
                    sleep(Duration::from_millis(300)).await;
                }
            }
        }
    }

    async fn internal_handle_new_connection(&self, stream: TcpStream) -> Result<()> {
        let (read, mut write) = stream.into_split();
        let msg = encode_to_vec(Message::ClientId(self.current_node_id), self.config)?;
        write.write_u32(msg.len() as u32).await?;
        write.write_all(&msg).await?;
        *self.read_stream.lock().await = Some(BufReader::new(read));
        *self.write_stream.lock().await = Some(BufWriter::new(write));
        Ok(())
    }

    async fn await_for_external_tcp_stream(&self) {
        loop {
            match self.external_stream.lock().await.take() {
                Some(stream) => {
                    let (read, write) = stream.into_split();
                    *self.read_stream.lock().await = Some(BufReader::new(read));
                    *self.write_stream.lock().await = Some(BufWriter::new(write));
                    break;
                }
                None => {
                    sleep(Duration::from_millis(300)).await;
                }
            }
        }
    }

    async fn internal_send(&self, msg: &Message) -> Result<()> {
        match self.write_stream.lock().await.as_mut() {
            Some(stream) => {
                let encoded_msg = encode_to_vec(&msg, self.config)?;
                stream.write_u32(encoded_msg.len() as u32).await?;
                stream.write_all(&encoded_msg).await?;
                stream.flush().await?;
            }
            None => {
                tracing::warn!("Message not sent due to stream not ready {:?}", msg);
            }
        }
        Ok(())
    }

    async fn internal_recv(&self) -> Result<Message> {
        let mut lock = self.read_stream.lock().await;
        let read_stream = lock.as_mut().ok_or(anyhow!("Missing stream"))?;
        let msg_size = read_stream.read_u32().await?;
        let mut buf = vec![0; msg_size as usize];
        read_stream.read_exact(&mut buf).await?;
        let msg = decode_from_slice(&buf, self.config)?;
        Ok(msg.0)
    }
}

impl NodeStream for TcpNodeStream {
    type Stream = TcpStream;

    fn new(identifier: NodeIdentifier, current_node_id: u64) -> Self {
        Self {
            identifier,
            current_node_id,
            config: Configuration::default(),
            read_stream: Arc::new(Mutex::new(None)),
            write_stream: Arc::new(Mutex::new(None)),
            external_stream: Arc::new(Mutex::new(None)),
        }
    }

    async fn send(&self, msg: Message) {
        match self.internal_send(&msg).await {
            Ok(_) => {}
            Err(err) => {
                tracing::error!("Error sending message: {err}")
            }
        }
    }

    async fn recv(&self) -> Message {
        loop {
            match self.internal_recv().await {
                Ok(msg) => {
                    tracing::debug!("Received message {:?} for id {}", msg, self.identifier.id);
                    return msg;
                }
                Err(err) => {
                    tracing::error!("Error receiving message: {err}");
                    self.reconnect().await;
                }
            }
        }
    }

    async fn push_stream(&self, stream: TcpStream) {
        self.external_stream.lock().await.replace(stream);
    }

    #[tracing::instrument(level = "debug", skip(get_node_stream))]
    async fn handle_new<F, N>(&self, get_node_stream: F)
    where
        F: Fn(u64) -> N,
        N: NodeStream<Stream = Self::Stream> + Send,
    {
        tracing::info!("Binding for address",);
        let tcp_listener = TcpListener::bind(self.identifier.addr)
            .await
            .expect("Could not bind");
        loop {
            match tcp_listener.accept().await {
                Ok(stream) => {
                    tracing::info!("New connection");
                    let mut stream = stream.0;
                    let msg_size = stream.read_u32().await;
                    let Ok(msg_size) = msg_size else {
                        tracing::error!(
                            "Erro reading for id {}: {}",
                            0,
                            msg_size.expect_err("Already checked for err")
                        );
                        sleep(Duration::from_millis(300)).await;
                        continue;
                    };
                    let mut buf = vec![0; msg_size as usize];
                    let read = stream.read_exact(&mut buf).await;
                    let Ok(_) = read else {
                        tracing::error!(
                            "Erro reading for id {}: {}",
                            0,
                            read.expect_err("Already checked for err")
                        );
                        sleep(Duration::from_millis(300)).await;
                        continue;
                    };
                    match decode_from_slice::<Message, _>(&buf, self.config) {
                        Ok((Message::ClientId(id), _)) => {
                            tracing::debug!("Received message {:?}", Message::ClientId(id),);
                            let node_stream = get_node_stream(id);
                            node_stream.push_stream(stream).await;
                        }
                        Ok(msg) => {
                            tracing::error!("Unexpected message received {:?}", msg);
                        }
                        Err(err) => {
                            tracing::error!("Error decoding message for id {}: {}", 0, err);
                        }
                    }
                }
                Err(err) => {
                    tracing::error!("Error connecting to node {}: {}", 0, err);
                    sleep(Duration::from_millis(300)).await;
                }
            }
        }
    }
}

#[derive(Debug, Clone, Encode, Decode)]
enum Message {
    ClientId(u64),
    RequestVote {
        term: u32,
        candidate_id: u64,
        last_log_index: u64,
        last_log_term: u32,
    },
    RequestVoteResponse {
        term: u32,
        vote_granted: bool,
    },
    AppendEntries {
        term: u32,
        leader_id: u64,
        prev_log_index: u64,
        prev_log_term: u32,
        entries: Vec<Entry>,
        leader_commit: u64,
    },
    AppendEntriesResponse {
        term: u32,
        success: bool,
    },
}

#[derive(Debug, Clone, Encode, Decode)]
struct Entry {
    index: u64,
    term: u32,
    operation: Operation,
}

#[derive(Debug, Clone, Encode, Decode)]
enum Operation {
    Set { key: String, value: Vec<u8> },
    Delete { key: String },
    Get { key: String },
}
