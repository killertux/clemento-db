use lsm::LSM;
use rand::{seq::SliceRandom, thread_rng};
use std::borrow::Cow;

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let mut lsm = LSM::new(512 * 1024 * 1024, 4, "./".to_string(), false).await?;

    let mut keys = vec![0u16];
    let n_operations = 500000;
    let start_key = 0u16;
    let value = format!("Value for key {}#", start_key);
    lsm.put(
        Cow::Owned(start_key.to_be_bytes().to_vec().into()),
        value.into(),
    )
    .await?;

    let mut write_stats = Stats::new();
    let mut read_stats = Stats::new();

    for _ in 0..n_operations {
        let read_operation: bool = rand::random();
        if read_operation {
            let key = keys.choose(&mut thread_rng()).unwrap();
            let start = std::time::Instant::now();
            let _value = lsm.get(&key.to_be_bytes().to_vec().into()).await?;
            let elapsed = start.elapsed().as_micros() as u64;
            read_stats.update(elapsed);
            // assert!(
            //     value.unwrap().as_ref()
            //         == format!("Value for key {}#{:?}#", key, (0..*key).collect::<Vec<_>>())
            //             .as_bytes()
            // );
            if read_stats.n_operations % 10000 == 0 {
                println!("#####");
                println!("Read stats:");
                read_stats.print();
            }
        } else {
            let key: u16 = rand::random();
            let value = format!("Value for key {}#{:?}", key, (0..key).collect::<Vec<_>>());
            let start = std::time::Instant::now();
            lsm.put(Cow::Owned(key.to_be_bytes().to_vec().into()), value.into())
                .await?;
            let elapsed = start.elapsed().as_micros() as u64;
            write_stats.update(elapsed);
            keys.push(key);
            if write_stats.n_operations % 10000 == 0 {
                println!("#####");
                println!("Write stats:");
                write_stats.print();
            }
        }
    }
    lsm.close().await?;
    Ok(())
}

struct Stats {
    n_operations: u64,
    total_time: u64,
    max_time: u64,
    min_time: u64,
    times: Vec<u64>,
}

impl Stats {
    fn new() -> Self {
        Self {
            n_operations: 0,
            total_time: 0,
            max_time: 0,
            min_time: u64::MAX,
            times: Vec::new(),
        }
    }

    fn update(&mut self, time: u64) {
        self.n_operations += 1;
        self.total_time += time;
        self.max_time = self.max_time.max(time);
        self.min_time = self.min_time.min(time);
        self.times.push(time);
    }

    fn avg_time(&self) -> f64 {
        self.total_time as f64 / self.n_operations as f64
    }

    fn print(&mut self) {
        self.times.sort();
        println!("Total operations: {}", self.n_operations);
        println!("Total time: {}us", self.total_time);
        println!("Max time: {}us", self.max_time);
        println!("Min time: {}us", self.min_time);
        println!("Avg time: {}us", self.avg_time());
        println!("P50 time: {}us", self.percentile(50));
        println!("P90 time: {}us", self.percentile(90));
        println!("P99 time: {}us", self.percentile(99));
    }

    fn percentile(&self, percentile: usize) -> u64 {
        let index = ((self.times.len() * percentile) / 100) as usize;
        self.times[index]
    }
}
