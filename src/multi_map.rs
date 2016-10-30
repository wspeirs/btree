use ::{KeyType, ValueType};

use wal_file::KeyValuePair;

use std::collections::{BTreeMap, BTreeSet};
use std::collections::btree_map::Entry::Occupied;
use std::collections::btree_map;
use std::collections::btree_set;
use std::collections::btree_set::Iter;

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

    /*
     * It would be nice to return a "generic" iterator here
     * not one tied to our underlying implementation. Not really
     * sure how: https://goo.gl/9sisAb
     */
    pub fn get(&self, key: &K) -> Option<Iter<V>> {
        return self.multi_map.get(key).map(|set| set.iter());
    }

    pub fn contains_key(&self, key: &K) -> bool {
        match self.get(key) {
            Some(_) => true,
            None => false
        }
    }

    /*
     * Might want to re-think this and return an Error
     * as there isn't a great way to tell the user that a
     * key or value wasn't found
     */
    pub fn delete(&mut self, key: K, value: V) -> usize {
        if let Occupied(mut entry) = self.multi_map.entry(key) {

            if entry.get_mut().remove(&value) {
                self.count -= 1;            
            }

            if entry.get().is_empty() {
                entry.remove_entry();
            }
        }

        return self.count;
    }

    pub fn size(&self) -> usize {
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
    use multi_map::MultiMap;

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

    #[test]
    fn test_get() {
        let mut mmap = MultiMap::<i32,String>::new();
        
        assert!(mmap.insert(12, String::from("abc")) == 1);
        assert!(mmap.insert(23, String::from("abc")) == 2);
        assert!(mmap.insert(23, String::from("def")) == 3);

        let mut it1 = mmap.get(&12).unwrap();

        assert!(it1.next().unwrap() == "abc");
        assert!(it1.next() == None);

        let mut it2 = mmap.get(&23).unwrap();

        assert!(it2.next().unwrap() == "abc");
        assert!(it2.next().unwrap() == "def");
        assert!(it2.next() == None);
    }

    #[test]
    fn test_delete() {
        let mut mmap = MultiMap::<i32,String>::new();

        assert!(mmap.insert(12, String::from("abc")) == 1);
        assert!(mmap.insert(23, String::from("abc")) == 2);
        assert!(mmap.insert(23, String::from("def")) == 3);

        assert!(mmap.size() == 3);

        assert!(mmap.delete(12, String::from("abc")) == 2);
        assert!(mmap.delete(23, String::from("abc")) == 1);
        assert!(mmap.delete(23, String::from("abc")) == 1); // should NOT find this one
        assert!(mmap.delete(23, String::from("def")) == 0);

        assert!(mmap.size() == 0);

        let mut it = mmap.into_iter();

        assert!(it.next() == None);
    }
}


