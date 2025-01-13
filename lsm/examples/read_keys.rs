use lsm::KeyReader;
use tokio::fs::File;

#[tokio::main]
pub async fn main() -> Result<(), std::io::Error> {
    let base_path = "./".to_string();
    let level_id = 0;
    let file_id = 0;
    let mut reader = KeyReader {
        keys_reader: File::open(format!("{base_path}/{}_{:08}.sst_keys", level_id, file_id))
            .await?,
        n_entries: 100000,
        read_entries: 0,
    };
    while let Some(key) = reader.read().await? {
        let n_key = u64::from_be_bytes(key.as_ref().try_into().unwrap());
        println!("Key: {}", n_key);
    }
    Ok(())
}
