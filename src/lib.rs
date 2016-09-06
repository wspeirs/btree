extern crate bincode;
extern crate rustc_serialize;

use bincode::SizeLimit;
use bincode::rustc_serialize::{encode, decode};
use rustc_serialize::{Encodable, Decodable};

use std::error::Error;
use std::fs::File;
use std::io::Read;
use std::fs::OpenOptions;
use std::mem::{size_of};
use std::cmp::max;

const NUM_CHILDREN: usize = 32;

// specify the types for the keys & values
pub trait KeyType: Ord + Encodable + Decodable {}
pub trait ValueType: Encodable + Decodable {}

#[derive(RustcEncodable, RustcDecodable, PartialEq)]
enum Payload<K: Ord, V: ValueType> {
        Value(V),
        Children([(K,u64); NUM_CHILDREN]),
    }

#[derive(RustcEncodable, RustcDecodable, PartialEq)]
struct Node<K: KeyType, V: ValueType> {
    key: K,
    parent: u64,
    payload: Payload<K,V>, // either children, or actual values
}

pub struct BTree<K: KeyType, V: ValueType> {
    fd: File, // the file backing the whole thing
    first: u64, // first node in the linked-list of values
    root: Node<K,V>,
}

impl <K: KeyType, V: ValueType> BTree<K, V> {
    pub fn new(key_size: usize, value_size: usize) -> Result<BTree<K,V>, Box<Error>> {
        let mut file = try!(OpenOptions::new()
                                  .read(true)
                                  .write(true)
                                  .create(true)
                                  .open("btree.dat"));

        let total_size = key_size + size_of::<u64>() + max(value_size, (key_size+size_of::<u64>()) * NUM_CHILDREN);
        let mut buff = Vec::with_capacity(total_size);
        
        try!(file.read_exact(&mut buff));

        let root_node: Node<K,V> = decode(&buff[..]).unwrap();

        Ok(BTree{ fd: file, first: 0, root: root_node })
    }
}



#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        println!("It works!!!")
    }
}
