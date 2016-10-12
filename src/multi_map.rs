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
        let mut key_it = self.multi_map.iter().peekable();
        let value_it = match key_it.peek() {
            Some(&(_,v)) => Some(v.iter()),
            None => None // empty::<btree_set::Iter<'a,V>>()
        };

        MultiMapIterator{key_it: key_it, value_it: value_it}
    }
}

impl <'a, K: KeyType, V: ValueType> Iterator for MultiMapIterator<'a,K,V> {
    type Item = KeyValuePair<K,V>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.value_it {
            Some(mut i) => {
                let &cur_key = self.key_it.peek().unwrap().0;
                let cur_val = i.next();

                match cur_val {
                    Some(&v) => return Some(KeyValuePair{key: cur_key, value: v}),
                    None => {
                        self.key_it.next(); // increment our key iterator

                        match self.key_it.peek() {
                            Some(&(&k,v)) => {
                                self.value_it = Some(v.iter());
                                let &cur_val = self.value_it.unwrap().next().unwrap();
                                return Some(KeyValuePair{key: k, value: cur_val});
                            },
                            None => return None
                        }
                    }
                }
            },
            None => return None // when the value iterator is None, we're done
        }
    }
}

