extern crate bincode;
extern crate rustc_serialize;

use rustc_serialize::{Encodable, Decodable};

use ::{KeyType, ValueType};

use std::collections::{BTreeMap, BTreeSet};
use std::collections::btree_map::Iter as MIter;
use std::collections::btree_set::Iter as SIter;
use std::iter::Peekable;

#[derive(RustcEncodable, RustcDecodable, PartialEq)]
struct WALRecord<K: KeyType, V: ValueType> {
    key: K,
    value: V,
}

impl <K: KeyType, V: ValueType> Iterator for WALRecord<K,V> {
    type Item = WALRecord<K,V>;

    fn next(&mut self) -> Option<Self::Item> {

    }
}


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



