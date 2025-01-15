use lsm::LSM;
use rand::{seq::SliceRandom, thread_rng};
use std::borrow::Cow;

#[tokio::main]
pub async fn main() -> Result<(), std::io::Error> {
    let mut lsm = LSM::new(4 * 1024 * 1024, 2, "./".to_string())
        .await
        .unwrap();

    let mut keys = vec![0u64];
    let n_operations = 1000000;
    let start_key = 0u64;
    let value = format!("Value for key {}#", start_key);
    lsm.put(
        Cow::Owned(start_key.to_be_bytes().to_vec().into()),
        value.into(),
    )
    .await
    .unwrap();

    let mut write_stats = Stats::new();
    let mut read_stats = Stats::new();

    for _ in 0..n_operations {
        let read_operation: bool = rand::random();
        if read_operation {
            let key = keys.choose(&mut thread_rng()).unwrap();
            let start = std::time::Instant::now();
            let value = lsm.get(&key.to_be_bytes().to_vec().into()).await.unwrap();
            let elapsed = start.elapsed().as_millis() as u64;
            read_stats.update(elapsed);
            assert!(value.unwrap().as_ref() == format!("Value for key {}#", key).as_bytes());
            if read_stats.n_operations % 10000 == 0 {
                println!("#####");
                println!("Read stats:");
                read_stats.print();
            }
        } else {
            let key: u64 = rand::random();
            let value = format!("Value for key {}#", key);
            let start = std::time::Instant::now();
            lsm.put(Cow::Owned(key.to_be_bytes().to_vec().into()), value.into())
                .await
                .unwrap();
            let elapsed = start.elapsed().as_millis() as u64;
            write_stats.update(elapsed);
            keys.push(key);
            if write_stats.n_operations % 10000 == 0 {
                println!("#####");
                println!("Write stats:");
                write_stats.print();
            }
        }
    }

    Ok(())
}

struct Stats {
    n_operations: u64,
    total_time: u64,
    max_time: u64,
    min_time: u64,
}

impl Stats {
    fn new() -> Self {
        Self {
            n_operations: 0,
            total_time: 0,
            max_time: 0,
            min_time: u64::MAX,
        }
    }

    fn update(&mut self, time: u64) {
        self.n_operations += 1;
        self.total_time += time;
        self.max_time = self.max_time.max(time);
        self.min_time = self.min_time.min(time);
    }

    fn avg_time(&self) -> f64 {
        self.total_time as f64 / self.n_operations as f64
    }

    fn print(&self) {
        println!("Total operations: {}", self.n_operations);
        println!("Total time: {}ms", self.total_time);
        println!("Max time: {}ms", self.max_time);
        println!("Min time: {}ms", self.min_time);
        println!("Avg time: {}ms", self.avg_time());
    }
}
