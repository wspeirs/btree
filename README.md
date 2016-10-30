# Log Structured Merge B+ Tree (LSMBT)

This an implementation of two different data structures:
* [Log Structured Merge Tree](https://en.wikipedia.org/wiki/Log-structured_merge-tree)
* [B+Tree](https://en.wikipedia.org/wiki/B%2B_tree)

The implementation also leverages a [write-ahead log](https://en.wikipedia.org/wiki/Write-ahead_logging) to ensure that data is not lost.

## Basic Architecture

When you create a LSMBT 2 files are created: a blank B+ Tree file, and a blank WAL file. An in-memory [BTreeMap](https://doc.rust-lang.org/stable/std/collections/struct.BTreeMap.html) is also constructed. Each method of the LSMBT is outlined below

### Insert (key,value)
When a (key,value) pair is added to the LSMBT the following occurs:
1. The (key,value) pair is written to the WAL file.
1. The (key,value) pair is added to the in-memory BTree. If the size of the in-memory BTree hits a particular threshold, then
  1. The in-memory BTree is merged with the on-disk B+Tree to create a new on-disk B+Tree.
  1. The in-memory BTree and the WAL file are both truncated.

### Get Values
Because a key can be associated with a set (no duplicate values per key) of values, the `get` method returns a list of values:

1. Collect all of the values associated with a given key in the in-memory BTree.
1. Collect all of the values associated with a given key in the on-disk B+Tree.
1. Return all the unique values

### Delete Value
Again, because a key can be associated with a set of values, the value to be removed must be supplied during a delete:

1. Remove the value from the in-memory BTree. If it is the only value associated with the key, then remove the key as well.
1. Mark the value in the on-disk B+Tree as deleted. (The value isn't actually removed until a compaction occurs.)

