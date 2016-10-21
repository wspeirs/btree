use wal_file::WALFile;

/*
const NUM_CHILDREN: usize = 32;
const FILE_HEADER: &'static str = "B+Tree\0";
const CURRENT_VERSION: u8 = 0x01;


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

*/

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


// total hack to get things going
pub type OnDiskBTree<K,V> = WALFile<K,V>;

/*

        // check to see if this is a new file
        if metadata.len() == 0 {
            // write out our header
            try!(tree_file.write(FILE_HEADER.as_bytes()));

            // write out our version
            try!(tree_file.write(&[CURRENT_VERSION]));

            // construct and return our BTree object
            Ok(BTree{tree_file_path: tree_file_path,
                     tree_file: tree_file,
                     wal_file: wal_file,
                     root: None,
                     max_key_size: max_key_size,
                     max_value_size: max_value_size,
                     mem_tree: mem_tree
            })
        } else {
            let mut version_string = vec![0; 8];

            try!(tree_file.read_exact(&mut version_string));

            // make sure we've opened a proper file
            if try!(str::from_utf8(&version_string[0..FILE_HEADER.len()])) != FILE_HEADER ||
               version_string[FILE_HEADER.len()] != CURRENT_VERSION {
                return Err(From::from(std::io::Error::new(ErrorKind::InvalidData, "Invalid BTree file or BTree version")));
            }

            let mut buff = vec![0; node_size];

            // make sure we have a root node to read
            if metadata.len() < (version_string.len() + node_size) as u64 {
                // if we don't have a root node yet, just return
                return Ok(BTree{tree_file_path: tree_file_path,
                                tree_file: tree_file,
                                wal_file: wal_file,
                                root: None,
                                max_key_size: max_key_size,
                                max_value_size: max_value_size,
                                mem_tree: mem_tree
                });
            }

            // seek node_size in from the end of the file to read the root node
            try!(tree_file.seek(SeekFrom::End((node_size as isize * -1) as i64)));
            try!(tree_file.read_exact(&mut buff));

            let root_node: Node<K,V> = try!(decode(&buff[..]));

            Ok(BTree{tree_file_path: tree_file_path,
                     tree_file: tree_file,
                     wal_file: wal_file,
                     root: Some(root_node),
                     max_key_size: max_key_size,
                     max_value_size: max_value_size,
                     mem_tree: mem_tree
            })
        }


*/
