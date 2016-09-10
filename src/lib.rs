extern crate bincode;
extern crate rustc_serialize;

use bincode::SizeLimit;
use bincode::rustc_serialize::{encode, decode};
use rustc_serialize::{Encodable, Decodable};

use std::cmp::max;
use std::convert::From;
use std::error::Error;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{Read, Write, Seek, SeekFrom, ErrorKind};
use std::mem::{size_of};
use std::str;

const NUM_CHILDREN: usize = 32;
const FILE_HEADER: &'static str = "B+Tree\0";
const CURRENT_VERSION: u8 = 0x01;

// specify the types for the keys & values
pub trait KeyType: Ord + Encodable + Decodable {}
pub trait ValueType: Encodable + Decodable {}

// provide generic implementations
impl<T> KeyType for T where T: Ord + Encodable + Decodable {}
impl<T> ValueType for T where T: Encodable + Decodable {}

#[derive(RustcEncodable, RustcDecodable, PartialEq)]
enum Payload<K: KeyType, V: ValueType> {
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
/// | 0x42 0x2b 0x54 0x72 | 0x65 0x65 0x00 0xVV |
/// | B    +    T    r    | e    e    \0   0xVV |
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
    fd: File,                // the file backing the whole thing
    root: Option<Node<K,V>>, // optional in-memory copy of the root node
    key_size: usize,         // the size of the key in bytes
    value_size: usize,       // the size of the value in bytes
}

impl <K: KeyType, V: ValueType> BTree<K, V> {
    pub fn new(file_path: &str, key_size: usize, value_size: usize) -> Result<BTree<K,V>, Box<Error>> {
        let mut file = try!(OpenOptions::new()
                                  .read(true)
                                  .write(true)
                                  .create(true)
                                  .open(file_path));

        // check to see if this is a new file
        let metadata = try!(file.metadata());

        println!("FILE HAS LENGTH: {}", metadata.len());

        if metadata.len() == 0 {
            // write out our header
            try!(file.write(FILE_HEADER.as_bytes()));
            // write out our version
            try!(file.write(&[CURRENT_VERSION]));

            Ok(BTree{fd: file, key_size: key_size, value_size: value_size, root: None})
        } else {
            // make sure we've opened a proper file
            let mut version_string = vec![0; 8];

            try!(file.read_exact(&mut version_string));

            if try!(str::from_utf8(&version_string[0..FILE_HEADER.len()])) != FILE_HEADER ||
               version_string[FILE_HEADER.len()] != CURRENT_VERSION {
                return Err(From::from(std::io::Error::new(ErrorKind::InvalidData, "Invalid BTree file version")));
            }

            // total size of a Node
            let total_size: usize = (key_size + size_of::<u64>() + max(value_size, (key_size+size_of::<u64>()) * NUM_CHILDREN)) as usize;
            let mut buff = vec![0; total_size];

            // make sure we have a root node to read
            if metadata.len() < (version_string.len() + total_size) as u64 {
                // if we don't have a root node yet, just return
                return Ok(BTree{fd: file, key_size: key_size, value_size: value_size, root: None});
            }
            
            // seek total_size in from the end of the file to read the root node
            try!(file.seek(SeekFrom::End((total_size as isize * -1) as i64)));
            try!(file.read_exact(&mut buff));

            let root_node: Node<K,V> = try!(decode(&buff[..]));

            Ok(BTree{fd: file, key_size: key_size, value_size: value_size, root: Some(root_node)})
        }
    }
}



#[cfg(test)]
mod tests {
    use std::fs;
    use ::BTree;


    #[test]
    fn new_blank_file() {
        // make sure we remove any old files
        fs::remove_file("/tmp/btree_test.btr");

        BTree::<u8, u8>::new("/tmp/btree_test.btr", 1, 1).unwrap();
    }

    #[test]
    fn new_existing_file() {
        new_blank_file();  // assume this works

        let btree = BTree::<u8, u8>::new("/tmp/btree_test.btr", 1, 1).unwrap();
    }
}
