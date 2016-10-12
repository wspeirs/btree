use ::{KeyType, ValueType};

use wal_file::KeyValuePair;

use std::iter::{Peekable, empty, Map};
use std::collections::{BTreeMap, BTreeSet};
use std::collections::btree_map;
use std::collections::btree_set;

pub struct MultiMap<K: KeyType, V: ValueType> {
    multi_map: BTreeMap<K, BTreeSet<V>>
}

pub struct MultiMapIterator<'a, K: KeyType + 'a, V: ValueType + 'a> {
    key_it: Peekable<btree_map::Iter<'a,K,BTreeSet<V>>>,
    value_it: btree_set::Iter<'a,V>,
}

impl <'a, K: KeyType, V: ValueType> MultiMap<K,V> {
    pub fn new() -> MultiMap<K,V> {
        return MultiMap{multi_map: BTreeMap::<K,BTreeSet<V>>::new()};
    }
}

impl <'a, K: KeyType, V: ValueType> IntoIterator for &'a mut MultiMap<K,V> {
    type Item = KeyValuePair<K,V>;
    type IntoIter = MultiMapIterator<'a,K,V>;

    fn into_iter(self) -> Self::IntoIter {
        let mut key_it = self.multi_map.iter().peekable();
        let value_it = match key_it.peek() {
            Some(&(k,v)) => v.iter(),
            None => empty::<btree_set::Iter<'a,V>>()
        };

        MultiMapIterator{key_it: key_it, value_it: value_it}
    }
}

impl <'a, K: KeyType, V: ValueType> Iterator for MultiMapIterator<'a,K,V> {
    type Item = KeyValuePair<K,V>;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

