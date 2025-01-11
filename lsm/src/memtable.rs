use bytes::Bytes;
use std::borrow::Cow;

pub(crate) struct Memtable {
    size: usize,
    data: Vec<(Key, Value)>,
}

impl Memtable {
    pub fn new() -> Self {
        Self {
            size: 0,
            data: vec![],
        }
    }

    pub fn put(&mut self, key: Cow<'_, Bytes>, value: Value) {
        let key_size = key.len();
        if key_size == 0 {
            return;
        }
        let value_size = value.size();
        match self.data.binary_search_by(|entry| entry.0.cmp(&key)) {
            Ok(index) => {
                let old_value_size = self.data[index].1.size();
                self.data[index].1 = value;
                self.size -= old_value_size;
                self.size += value_size;
            }
            Err(index) => {
                self.size += key_size + value_size;
                self.data.insert(index, (key.into_owned(), value))
            }
        }
    }

    pub fn get(&self, key: &Bytes) -> Option<&Value> {
        match self.data.binary_search_by(|entry| entry.0.cmp(&key)) {
            Ok(index) => Some(&self.data[index].1),
            Err(_) => None,
        }
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn data(self) -> Vec<(Key, Value)> {
        self.data
    }
}

pub(crate) type Key = Bytes;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Value {
    TombStone,
    Data(Bytes),
}

impl Value {
    pub fn size(&self) -> usize {
        match self {
            Value::TombStone => 1,
            Value::Data(value) => value.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_non_existent() {
        let lsm = Memtable::new();
        let key = Bytes::from("key");
        assert_eq!(lsm.get(&key), None);
        assert_eq!(lsm.size(), 0);
    }

    #[test]
    fn test_put_get() {
        let mut lsm = Memtable::new();
        let key = Bytes::from("key");
        let value = Bytes::from("value");
        lsm.put(Cow::Borrowed(&key), Value::Data(value.clone()));
        assert_eq!(lsm.get(&key), Some(&Value::Data(value)));
        assert_eq!(lsm.size(), 8);
    }

    #[test]
    fn test_put_existent_should_overwrite() {
        let mut lsm = Memtable::new();
        let key = Bytes::from("key");
        let value_1 = Bytes::from("value 1");
        let value_2 = Bytes::from("value 2");
        lsm.put(Cow::Borrowed(&key), Value::Data(value_1.clone()));
        lsm.put(Cow::Borrowed(&key), Value::Data(value_2.clone()));
        assert_eq!(lsm.get(&key), Some(&Value::Data(value_2)));
        assert_eq!(lsm.size(), 10);
        lsm.put(Cow::Borrowed(&key), Value::TombStone);
        assert_eq!(lsm.get(&key), Some(&Value::TombStone));
        assert_eq!(lsm.size(), 4);
    }
}
