extern crate bincode;
extern crate rustc_serialize;

use bincode::SizeLimit;
use bincode::rustc_serialize::{encode, decode};

use ::{KeyType, ValueType};

use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write, ErrorKind, Seek, SeekFrom};
use std::io::Error as IOError;
use std::marker::PhantomData;
use std::cmp::Ordering;

#[derive(RustcEncodable, RustcDecodable, PartialEq)]
pub struct KeyValuePair<K: KeyType, V: ValueType> {
    pub key: K,
    pub value: V,
}

impl <K: KeyType, V: ValueType> PartialOrd for KeyValuePair<K,V> {
    fn partial_cmp(&self, other: &KeyValuePair<K,V>) -> Option<Ordering> {
        if self.key == other.key {
            Some(self.value.cmp(&other.value))
        } else {
            Some(self.key.cmp(&other.key))
        }
    }
}

pub struct RecordFile<K: KeyType, V: ValueType> {
    fd: File,  // the file
    key_size: usize,
    value_size: usize,
    _k_marker: PhantomData<K>,
    _v_marker: PhantomData<V>
}

pub struct RecordFileIterator<'a, K: KeyType + 'a, V: ValueType + 'a> {
    wal_file: &'a mut RecordFile<K,V>,  // the file
}

impl <K: KeyType, V: ValueType> RecordFile<K,V> {
    pub fn new(wal_file_path: &String, key_size: usize, value_size: usize) -> Result<RecordFile<K,V>, Box<Error>> {
        let wal_file = try!(OpenOptions::new().read(true).write(true).create(true).open(wal_file_path));

        return Ok(RecordFile{fd: wal_file,
                          key_size: key_size,
                          value_size: value_size,
                          _k_marker: PhantomData,
                          _v_marker: PhantomData});
    }

    pub fn is_new(&self) -> Result<bool, Box<Error>> {
        Ok(try!(self.fd.metadata()).len() == 0)
    }

    /// Returns the number of records in the WAL file
    pub fn count(&self) -> Result<u64, Box<Error>> {
        let file_size = try!(self.fd.metadata()).len();
        let rec_size: u64 = (self.key_size + self.value_size) as u64;

        if file_size % rec_size != 0 {
            Err(From::from(IOError::new(ErrorKind::InvalidData, "File size is NOT a multiple of key size + value size")))
        } else {
            Ok(file_size/rec_size)
        }
    }

    pub fn insert_record(&mut self, kv: &KeyValuePair<K,V>) -> Result<(), Box<Error>> {
        // encode the record
        let record_size = self.key_size + self.value_size;
        let mut buff = try!(encode(&kv, SizeLimit::Bounded(record_size as u64)));

        // padd it out to the max size
        if buff.len() > self.key_size + self.value_size {
            return Err(From::from(IOError::new(ErrorKind::InvalidData, "Key and value size are too large")));
        } else {
            let diff = (self.key_size + self.value_size) - buff.len();
            buff.extend(vec![0; diff]);
        }

        match self.fd.write_all(&buff) {
            Ok(_) => Ok( () ),
            Err(e) => Err(From::from(e))
        }
    }
}

impl <'a, K: KeyType, V: ValueType> IntoIterator for &'a mut RecordFile<K,V> {
    type Item = KeyValuePair<K,V>;
    type IntoIter = RecordFileIterator<'a, K,V>;

    fn into_iter(self) -> Self::IntoIter {
        // seek back to the start
        self.fd.seek(SeekFrom::Start(0));

        // create our iterator
        RecordFileIterator{wal_file: self}
    }
}

impl <'a, K: KeyType, V: ValueType> Iterator for RecordFileIterator<'a,K,V> {
    type Item = KeyValuePair<K,V>;

    fn next(&mut self) -> Option<Self::Item> {
        let total_size = self.wal_file.key_size + self.wal_file.value_size;
        let mut buff = vec![0; total_size];

        println!("Creating buffer: {}", total_size);

        // attempt to read a buffer's worth and decode
        match self.wal_file.fd.read_exact(&mut buff) {
            Ok(_) => {
                match decode(&buff) {
                    Ok(record) => Some(record),
                    Err(_) => None
                }
            },
            Err(e) => {
                println!("ERROR: {}", e);
                None
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use tests::gen_temp_name;
    use std::fs;
    use wal_file::{RecordFile, KeyValuePair};

    #[test]
    fn test_iterator() {
        let temp_path = gen_temp_name();
        let file_path = temp_path.to_owned() + ".wal";

        // create a new blank file
        let mut wal_file = RecordFile::new(&file_path, 20, 20).unwrap();

        assert!(wal_file.is_new().unwrap());

        let kv1 = KeyValuePair{key: "hello".to_owned(), value: "world".to_owned()};
        let kv2 = KeyValuePair{key: "foo".to_owned(), value: "bar".to_owned()};

        wal_file.insert_record(&kv1).unwrap();
        wal_file.insert_record(&kv2).unwrap();

        assert!(wal_file.count().unwrap() == 2);

        let mut wal_it = wal_file.into_iter();

        let it_kv1 = wal_it.next().unwrap();

        assert!(kv1.key == it_kv1.key);
        assert!(kv1.value == it_kv1.value);

        let it_kv2 = wal_it.next().unwrap();

        assert!(kv2.key == it_kv2.key);
        assert!(kv2.value == it_kv2.value);

        fs::remove_file(&file_path);
    }
}
