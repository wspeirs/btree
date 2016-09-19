extern crate bincode;
extern crate rustc_serialize;

use bincode::rustc_serialize::{encode, decode};
use rustc_serialize::{Encodable, Decodable};

use ::{KeyType, ValueType};

use std::collections::{BTreeMap, BTreeSet};
use std::collections::btree_map::Iter as MIter;
use std::collections::btree_set::Iter as SIter;
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::iter::Peekable;
use std::marker::PhantomData;

#[derive(RustcEncodable, RustcDecodable, PartialEq)]
struct KeyValuePair<K: KeyType, V: ValueType> {
    key: K,
    value: V,
}

struct WALIterator<K: KeyType, V: ValueType> {
    fd: File,  // the WAL file
    key_size: usize,
    value_size: usize,
    _k_marker: PhantomData<K>,
    _v_marker: PhantomData<V>
}

impl <K: KeyType, V: ValueType> WALIterator<K,V> {
    fn new(wal_file_path: String, key_size: usize, value_size: usize) -> Result<WALIterator<K,V>, Box<Error>> {
        let mut wal_file = try!(OpenOptions::new().read(true).create(false).open(wal_file_path));

        return Ok(WALIterator{fd: wal_file,
                              key_size: key_size,
                              value_size: value_size,
                              _k_marker: PhantomData,
                              _v_marker: PhantomData});
    } 
}

impl <K: KeyType, V: ValueType> Iterator for WALIterator<K,V> {
    type Item = KeyValuePair<K,V>;

    fn next(&mut self) -> Option<Self::Item> {
        let total_size = self.key_size + self.value_size;
        let mut buff = vec![0; total_size];

        // attempt to read a buffer's worth and decode
        match self.fd.read_exact(&mut buff) {
            Ok(_) => {
                match decode(&buff) {
                    Ok(record) => Some(record),
                    Err(e) => None
                }
            },
            Err(e) => None
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

