extern crate bincode;
extern crate rustc_serialize;

use bincode::SizeLimit;
use bincode::rustc_serialize::{encode, decode};
use rustc_serialize::{Encodable, Decodable};

use ::{KeyType, ValueType};

use std::collections::{BTreeMap, BTreeSet};
use std::collections::btree_map::Iter as MIter;
use std::collections::btree_set::Iter as SIter;
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write, ErrorKind};
use std::io::Error as IOError;
use std::iter::Peekable;
use std::marker::PhantomData;

#[derive(RustcEncodable, RustcDecodable, PartialEq)]
pub struct KeyValuePair<K: KeyType, V: ValueType> {
    pub key: K,
    pub value: V,
}

pub struct WALFile<K: KeyType, V: ValueType> {
    fd: File,  // the WAL file
    key_size: usize,
    value_size: usize,
    _k_marker: PhantomData<K>,
    _v_marker: PhantomData<V>
}

pub struct WALIterator<K: KeyType, V: ValueType> {
    wal_file: WALFile<K,V>,  // the WAL file
    _k_marker: PhantomData<K>,
    _v_marker: PhantomData<V>
}

impl <K: KeyType, V: ValueType> WALFile<K,V> {
    pub fn new(wal_file_path: String, key_size: usize, value_size: usize) -> Result<WALFile<K,V>, Box<Error>> {
        let wal_file = try!(OpenOptions::new().read(true).create(false).open(wal_file_path));

        return Ok(WALFile{fd: wal_file,
                          key_size: key_size,
                          value_size: value_size,
                          _k_marker: PhantomData,
                          _v_marker: PhantomData});
    }

    pub fn iter(&self) -> WALIterator<K,V> {
        WALIterator{wal_file: *self, _k_marker: self._k_marker, _v_marker: self._v_marker}
    }

    pub fn is_new(&self) -> Result<bool, Box<Error>> {
        return Ok(try!(self.fd.metadata()).len() == 0);
    }

    pub fn write_record(&self, kv: KeyValuePair<K,V>) -> Result<(), Box<Error>> {
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

        try!(self.fd.write_all(&buff))
    }
}

impl <K: KeyType, V: ValueType> Iterator for WALIterator<K,V> {
    type Item = KeyValuePair<K,V>;

    fn next(&mut self) -> Option<Self::Item> {
        let total_size = self.wal_file.key_size + self.wal_file.value_size;
        let mut buff = vec![0; total_size];

        // attempt to read a buffer's worth and decode
        match self.wal_file.fd.read_exact(&mut buff) {
            Ok(_) => {
                match decode(&buff) {
                    Ok(record) => Some(record),
                    Err(_) => None
                }
            },
            Err(_) => None
        }
    }
}

/*

struct MemoryRecordIterator<'a, 'b, K: KeyType, V: ValueType> {
    key_size: usize,
    value_size: usize,
    key_iter: Peekable<MIter<'a, K, BTreeSet<V>>>,
    value_iter: Option<Peekable<SIter<'b, V>>>
}

impl <'a, 'b, K: KeyType, V: ValueType> MemoryRecordIterator<'a, 'b, K, V> {
    fn new(key_size: usize, value_size: usize, mem_tree: BTreeMap<K, BTreeSet<V>>) -> MemoryRecordIterator<'a, 'b, K,V> {
        let key_it = mem_tree.iter().peekable();

        MemoryRecordIterator {key_size: key_size,
                              value_size: value_size,
                              key_iter: key_it, 
                              value_iter: key_it.peek().iter().peekable()}
    }
}

impl <'a, 'b, K: KeyType, V: ValueType> Iterator for MemoryRecordIterator<'a, 'b, K,V> {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {

    }
}

*/

#[cfg(test)]
mod tests {
    use tests::gen_temp_name;
    use std::fs;
    use std::fs::OpenOptions;
    use record_iterator::KeyValuePair;

    #[test]
    fn test_iterator() {
        let file_path = gen_temp_name();

        let wal = OpenOptions::new().read(true).write(true).create(true).open(&file_path).unwrap();

        let kv = KeyValuePair{key: "hello", value: "world"};

        let record_size = 20;
        let mut buff = try!(encode(&kv, SizeLimit::Bounded(record_size as u64)));

        // padd it out to the max size
        if buff.len() > record_size {
            panic!("Key and value size are too large");
        } else {
            let diff = record_size - buff.len();
            buff.extend(vec![0; diff]);
        }

        try!(self.wal_file.write_all(&buff));
    }
}
