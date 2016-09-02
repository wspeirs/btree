use std::fs::File;
use std::fs::OpenOptions;
use std::mem::size_of;

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
    root: Box<Node<K,V>>(),
}

impl <K: Ord, V> BTree<K, V> {
    pub fn new(keySize, valueSize) -> BTree<K,V> {
        let file = try!(OpenOptions::new()
                                  .read(true)
                                  .write(true)
                                  .create(true)
                                  .open("btree.dat"));

        let mut buff: [u8; keySize];
        file.read_exact(buff);

        BTree{ fd: file, first: 0 };
    }
}



#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        println!("It works!!!")
    }
}
