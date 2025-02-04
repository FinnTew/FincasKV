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
- [x] DB
  - [x] Put With TTL
  - [x] Batch Operation
  - [x] Redis DataStructure
    - [x] String
    - [x] List
    - [x] Hash
    - [x] Set
    - [x] ZSet
  - [x] FincasKV DB
- [ ] Network (Based on CloudWeGo Netpoll)
  - [x] TCP Conn
  - [x] Basic RESP Protocol
  - [x] Server
  - [ ] Stats Record
  - [ ] Handle For Redis DataStructure
    - [x] String
    - [ ] List
    - [x] Hash
    - [ ] Set
    - [ ] ZSet
- [ ] Raft