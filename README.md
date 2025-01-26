# FincasKV(Developing)

## Description

A distributed Key/Value storage based on Bitcask storage model, compatible with RESP protocol.

## TODO

- [x] Storage(Bitcask)
  - [x] Basic Bitcask
  - [x] Async Write
  - [x] Merge Ticker
  - [ ] Multi-Type-MemIndex
    - [x] BTree
    - [x] SkipList
    - [ ] SwissTable
  - [x] ShardMemIndex
  - [x] MemCache(implement LRUCache only)
  - [x] Use BloomFilter
- [ ] DB
  - [ ] Put With TTL
  - [ ] Transaction
    - [ ] MVCC
  - [ ] Batch
  - [ ] Redis DataStructure(Both Transaction and Without-Transaction): Plan to implemented by the way of multi-key
    - [ ] String
    - [ ] List
    - [ ] Hash
    - [ ] Set
    - [ ] ZSet
- [ ] Network & Supported RESP
- [ ] Raft