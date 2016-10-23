extern crate bincode;
extern crate rustc_serialize;
extern crate rand;

mod wal_file;
mod multi_map;
mod disk_btree;

use wal_file::{KeyValuePair, WAL, RecordFileIterator};
use multi_map::{MultiMap, MultiMapIterator};
use disk_btree::{OnDiskBTree};

use rustc_serialize::{Encodable, Decodable};

use std::error::Error;
use std::str;

const MAX_MEMORY_ITEMS: usize = 1000;

// specify the types for the keys & values
pub trait KeyType: Ord + Encodable + Decodable + Clone {}
pub trait ValueType: Ord + Encodable + Decodable + Clone  {}

// provide generic implementations

impl<T> KeyType for T where T: Ord + Encodable + Decodable + Clone {}
impl<T> ValueType for T where T: Ord + Encodable + Decodable + Clone {}

/// This struct holds all the pieces of the BTree mechanism
pub struct BTree<K: KeyType, V: ValueType> {
    tree_file_path: String,         // the path to the tree file
    max_key_size: usize,            // the max size of the key in bytes
    max_value_size: usize,          // the max size of the value in bytes
    wal_file: WAL<K,V>,         // write-ahead log for in-memory items
    mem_tree: MultiMap<K,V>,        // in-memory multi-map that gets merged with the on-disk BTree
    tree_file: Box<OnDiskBTree<K,V>>,    // the file backing the whole thing
}

impl <K: KeyType, V: ValueType> BTree<K, V> {
    pub fn new(tree_file_path: String, max_key_size: usize, max_value_size: usize) -> Result<BTree<K,V>, Box<Error>> {
        // create our in-memory multi-map
        let mut mem_tree = MultiMap::<K,V>::new();

        // construct the path to the WAL file for the in-memory multi-map
        let wal_file_path = tree_file_path.to_owned() + ".wal";

        // construct our WAL file
        let mut wal_file = try!(WAL::<K,V>::new(wal_file_path.to_owned(), max_key_size, max_value_size));

        // if we have a WAL file, replay it into the mem_tree
        if try!(wal_file.is_new()) {
            for kv in &mut wal_file {
                mem_tree.insert(kv.key, kv.value);
            }
        }


        // open the data file
        let tree_file = Box::new(try!(WAL::<K,V>::new(tree_file_path.to_owned(), max_key_size, max_value_size)));

        return Ok(BTree{tree_file_path: tree_file_path,
                        max_key_size: max_key_size,
                        max_value_size: max_value_size,
                        tree_file: tree_file,
                        wal_file: wal_file,
                        mem_tree: mem_tree});
    }

    /// Inserts a key into the BTree
    pub fn insert(&mut self, key: K, value: V) -> Result<(), Box<Error>> {
        let record = KeyValuePair{key: key, value: value};

        // should wrap this in a read-write lock
        try!(self.wal_file.write_record(&record));

        let KeyValuePair{key, value} = record;

        let size = self.mem_tree.insert(key, value);

        if size > MAX_MEMORY_ITEMS {
            try!(self.compact());
        }

        return Ok( () );
    }

    /// Merges the records on disk with the records in memory
    fn compact(&mut self) -> Result<(), Box<Error>>{
        // create a new on-disk BTree
        let mut new_tree_file = try!(WAL::<K,V>::new(self.tree_file_path + ".new", self.max_key_size, self.max_value_size));

        // get an iterator for the in-memory items
        let mut mem_iter = self.mem_tree.into_iter();

        // get an iterator to the on-disk items
        let mut disk_iter = self.tree_file.into_iter();

        loop {
            let mem_item = mem_iter.next();

        }
    }
}


#[cfg(test)]
mod tests {
    use std::fs;
    use std::fs::{OpenOptions, Metadata};
    use ::BTree;
    use rand::{thread_rng, Rng};


    pub fn gen_temp_name() -> String {
        let file_name: String = thread_rng().gen_ascii_chars().take(10).collect();

        return String::from("/tmp/") + &file_name + &String::from(".btr");
    }

    fn remove_files(file_path: String) {
        fs::remove_file(&file_path);
        fs::remove_file(file_path + ".wal");
    }


    #[test]
    fn new_blank_file() {
        let file_path = gen_temp_name();

        BTree::<u8, u8>::new(file_path.to_owned(), 1, 1).unwrap();

        // make sure our two files were created
        let btf = OpenOptions::new().read(true).write(false).create(false).open(&file_path).unwrap();
        assert!(btf.metadata().unwrap().len() == 8);

        let wal = OpenOptions::new().read(true).write(false).create(false).open(file_path.to_owned() + ".wal").unwrap();
        assert!(wal.metadata().unwrap().len() == 0);

        remove_files(file_path); // remove files assuming it all went well
    }

    #[test]
    fn new_existing_file() {
        let file_path = gen_temp_name();

        {
            BTree::<u8, u8>::new(file_path.to_owned(), 1, 1).unwrap();
        }

        let btree = BTree::<u8, u8>::new(file_path.to_owned(), 1, 1).unwrap();

        // check our file lengths from the struct
        assert!(btree.tree_file.metadata().unwrap().len() == 8);
        assert!(btree.wal_file.len().unwrap() == 0);

        remove_files(file_path); // remove files assuming it all went well
    }

    #[test]
    fn insert_new_u8() {
        let file_path = gen_temp_name();

        let mut btree = BTree::<u8, u8>::new(file_path.to_owned(), 1, 1).unwrap();

        let len = btree.insert(2, 3).unwrap(); // insert into a new file

        assert!(btree.wal_file.len().unwrap() == 2);
        assert!(btree.mem_tree.contains_key(&2));

        remove_files(file_path); // remove files assuming it all went well
    }

    #[test]
    fn insert_new_str() {
        let file_path = gen_temp_name();

        let mut btree = BTree::<String, String>::new(file_path.to_owned(), 15, 15).unwrap();

        // insert into a new file
        btree.insert("Hello".to_owned(), "World".to_owned()).unwrap();

        assert!(! btree.wal_file.is_new().unwrap());
        assert!(btree.mem_tree.contains_key(&String::from("Hello")));

        remove_files(file_path); // remove files assuming it all went well
    }

    #[test]
    fn insert_multiple() {
        let file_path = gen_temp_name();

        let mut btree = BTree::<String, String>::new(file_path.to_owned(), 15, 15).unwrap();

        // insert into a new file
        btree.insert("Hello".to_owned(), "World".to_owned()).unwrap();
        assert!(! btree.wal_file.is_new().unwrap());

        btree.insert("Hello".to_owned(), "Everyone".to_owned()).unwrap();
        assert!(! btree.wal_file.is_new().unwrap());

        remove_files(file_path); // remove files assuming it all went well
    }
}
