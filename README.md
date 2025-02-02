# FincasKV(Developing)

## Description

A distributed Key/Value storage based on Bitcask storage model, compatible with RESP protocol.

## TODO

- [x] Storage(Bitcask)
  - [x] Basic Bitcask
  - [x] Async Write
  - [x] Merge Ticker
  - [x] Multi-Type-MemIndex
    - [x] BTree
    - [x] SkipList
    - [x] SwissTable
  - [x] ShardMemIndex
  - [x] MemCache(implement LRUCache only)
  - [x] Use BloomFilter
- [ ] DB
  - [x] Put With TTL
  - [x] Batch Operation
  - [ ] Redis DataStructure: Plan to implemented by the way of multi-key
    - [x] String
    - [ ] List
    - [x] Hash
    - [ ] Set
    - [ ] ZSet
- [ ] Network & Supported RESP
- [ ] Raft