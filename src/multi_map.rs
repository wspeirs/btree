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
    cur_key: Option<&'a K>,
    key_it: btree_map::Iter<'a,K,BTreeSet<V>>,
    value_it: Option<btree_set::Iter<'a,V>>,
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
        let mut key_it = self.multi_map.iter();
        let cur_entry = key_it.next();

        // check to see if our map is empty
        if cur_entry.is_none() {
            return MultiMapIterator{cur_key: None, key_it: key_it, value_it: None};
        }

        // safe to call unwrap as we tested above
        let (cur_key, cur_set) = cur_entry.unwrap();

        return MultiMapIterator{cur_key: Some(cur_key), key_it: key_it, value_it: Some(cur_set.iter())};
    }
}

impl <'a, K: KeyType, V: ValueType> Iterator for MultiMapIterator<'a,K,V> {
    type Item = KeyValuePair<K,V>;

    fn next(&mut self) -> Option<Self::Item> {
        // this is our invariant, when it's None we've gone through everything
        if self.cur_key.is_none() {
            return None
        }

        // should be safe to call unwrap here, because we checked for None above
        let mut cur_val = self.value_it.as_mut().unwrap().next();

        // check to see if we've gone through everything in the set
        if cur_val.is_none() {
            let cur_entry = self.key_it.next(); // increment our key iterator

            if cur_entry.is_none() {
                self.cur_key = None; // set our invariant
                return None;
            }

            // safe to call unwrap because we checked it above
            let (cur_key, cur_set) = cur_entry.unwrap();

            self.cur_key = Some(cur_key); // set our key
            self.value_it = Some(cur_set.iter()); // set our value iterator
            cur_val = self.value_it.as_mut().unwrap().next(); // set our current value
        }

        return Some(KeyValuePair{key: self.cur_key.unwrap(), value: *(cur_val.unwrap())});
    }
}

