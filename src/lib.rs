use std::error::Error;
use std::fs::File;
use std::io::Read;
use std::fs::OpenOptions;
use std::mem::{size_of, transmute};
use std::cmp::max;

const NUM_CHILDREN: usize = 32;

enum Payload<K: Ord, V> {
        Value(V),
        Children([(K,u64); NUM_CHILDREN]),
    }

struct Node<K: Ord, V> {
    key: K,
    parent: u64,
    payload: Payload<K,V>, // either children, or actual values
}

pub struct BTree<K: Ord, V> {
    fd: File, // the file backing the whole thing
    first: u64, // first node in the linked-list of values
    root: Node<K,V>,
}

impl <K: Ord, V> BTree<K, V> {
    pub fn new(keySize: usize, valueSize: usize) -> Result<BTree<K,V>, Box<Error>> {
        let file = try!(OpenOptions::new()
                                  .read(true)
                                  .write(true)
                                  .create(true)
                                  .open("btree.dat"));

        let totalSize = keySize + size_of::<u64>() + max(valueSize, (keySize+size_of::<u64>()) * NUM_CHILDREN);
        let mut buff = Vec::with_capacity(totalSize);
        file.read_exact(&mut buff);

        let root_node: Node<K,V> = unsafe { transmute(buff) };

        BTree{ fd: file, first: 0, root: root_node };
    }
}



#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        println!("It works!!!")
    }
}
