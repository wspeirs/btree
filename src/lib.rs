extern crate bincode;
extern crate rustc_serialize;

use bincode::SizeLimit;
use bincode::rustc_serialize::{encode, decode};
use rustc_serialize::{Encodable, Decodable};

use std::error::Error;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
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

/// This struct represents an on-disk B+Tree. There are NUM_CHILDREN keys at each
/// level in the tree. The on-disk format is as follows where VV is the version
/// number:
/// |-------------------------------------------|
/// | 0x2b 0x42 0x72 0x54 | 0x65 0x65 0x00 0xVV |
/// |-------------------------------------------|
/// | smallest record in bincode format         |
/// |-------------------------------------------|
/// | ...                                       |
/// |-------------------------------------------|
/// | largest record in bincode format          |
/// |-------------------------------------------|
/// | internal nodes ...                        |
/// |-------------------------------------------|
/// | root node                                 |
/// |-------------------------------------------|
pub struct BTree<K: KeyType, V: ValueType> {
    fd: File,           // the file backing the whole thing
    root: Node<K,V>,    // in-memory copy of the root node
    key_size: usize,    // the size of the key in bytes
    value_size: usize,  // the size of the value in bytes
}

impl <K: KeyType, V: ValueType> BTree<K, V> {
    pub fn new(key_size: usize, value_size: usize) -> Result<BTree<K,V>, Box<Error>> {
        let mut file = try!(OpenOptions::new()
                                  .read(true)
                                  .write(true)
                                  .create(true)
                                  .open("btree.dat"));

        // make sure we've opened a proper file
        let mut version_string = Vec::with_capacity(8);

        try!(file.read_exact(&mut version_string));

        if version_string[0] != 0x2b {
            Err("Invalid BTree File")
        }

        // total size of a Node
        let total_size = key_size + size_of::<u64>() + max(value_size, (key_size+size_of::<u64>()) * NUM_CHILDREN);
        let mut buff = Vec::with_capacity(total_size);
        
        // seek total_size in from the end of the file to read the root node
        try!(file.seek(SeekFrom::End(total_size as i64)));
        try!(file.read_exact(&mut buff));

        let root_node: Node<K,V> = decode(&buff[..]).unwrap();

        Ok(BTree{ fd: file, key_size: key_size, value_size: value_size, root: root_node })
    }
}



#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        println!("It works!!!")
    }
}
