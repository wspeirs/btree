use ::{KeyType, ValueType};

use wal_file::KeyValuePair;

use std::iter::{Peekable, empty, Map};
use std::collections::{BTreeMap, BTreeSet};
use std::collections::btree_map;
use std::collections::btree_set;

pub struct MultiMap<K: KeyType, V: ValueType> {
    multi_map: BTreeMap<K, BTreeSet<V>>,
    count: usize  // total number of KV pairs
}

pub struct MultiMapIterator<'a, K: KeyType + 'a, V: ValueType + 'a> {
    cur_key: Option<&'a K>,
    key_it: btree_map::Iter<'a,K,BTreeSet<V>>,
    value_it: Option<btree_set::Iter<'a,V>>,
}

impl <'a, K: KeyType, V: ValueType> MultiMap<K,V> {
    pub fn new() -> MultiMap<K,V> {
        return MultiMap{multi_map: BTreeMap::<K,BTreeSet<V>>::new(), count: 0};
    }

    pub fn insert(&mut self, key: K, value: V) -> usize {
        self.count += 1;

        if let Some(set) = self.multi_map.get_mut(&key) {
            set.insert(value);
            return self.count;
        }
        
        let mut set = BTreeSet::<V>::new();

        set.insert(value);

        self.multi_map.insert(key, set);

        return self.count;
    }

    pub fn delete(&mut self, key: K, value: V) -> usize {
        if let Some(set) = self.multi_map.get_mut(&key) {
            if set.remove(&value) {
                self.count -= 1;            
            }

/* NOT WORKING: cannot borrow `self.multi_map` as mutable more than once at a time [E0499]
            if set.is_empty() {
                self.multi_map.remove(&key);
            }
*/
        }

        return self.count;
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

        return Some(KeyValuePair{key: self.cur_key.unwrap().clone(), value: cur_val.unwrap().clone()});
    }
}


#[cfg(test)]
mod tests {
    use multi_map::{MultiMap, MultiMapIterator};
    use wal_file::KeyValuePair;

    #[test]
    fn test_insert() {
        let mut mmap = MultiMap::<i32,String>::new();

        assert!(mmap.insert(12, String::from("abc")) == 1);
        assert!(mmap.insert(23, String::from("abc")) == 2);
        assert!(mmap.insert(23, String::from("def")) == 3);

        let mut it = mmap.into_iter();

        let e1 = it.next().unwrap();
        assert!(12 == e1.key);
        assert!(String::from("abc") == e1.value);
        
        let e2 = it.next().unwrap();
        assert!(23 == e2.key);
        assert!(String::from("abc") == e2.value);
        
        let e3 = it.next().unwrap();
        assert!(23 == e3.key);
        assert!(String::from("def") == e3.value);
    }
}


