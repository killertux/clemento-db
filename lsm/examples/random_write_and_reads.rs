use lsm::LSM;
use rand::seq::SliceRandom;
use std::borrow::Cow;

#[tokio::main]
pub async fn main() -> Result<(), std::io::Error> {
    let mut lsm = LSM::new(512 * 1024, 2, "./".to_string());
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

    for _ in 0..n_operations {
        let read_operation: bool = rand::random();
        if read_operation {
            let key = keys.choose(&mut rand::thread_rng()).unwrap();
            println!("Getting key {key}");
            let value = lsm.get(&key.to_be_bytes().to_vec().into()).await.unwrap();
            println!(
                "Got key {}",
                String::from_utf8_lossy(value.unwrap().as_ref())
            );
        } else {
            let key: u64 = rand::random();
            let value = format!("Value for key {}#", key);
            println!("Saving key");
            lsm.put(Cow::Owned(key.to_be_bytes().to_vec().into()), value.into())
                .await
                .unwrap();
            keys.push(key);
            println!("Saved key");
        }
    }

    Ok(())
}
