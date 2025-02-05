# FincasKV(Developing)

```text
  ______ _                     _  ____      __
 |  ____(_)                   | |/ /\ \    / /
 | |__   _ _ __   ___ __ _ ___| ' /  \ \  / / 
 |  __| | | '_ \ / __/ _` / __|  <    \ \/ /  
 | |    | | | | | (_| (_| \__ \ . \    \  /   
 |_|    |_|_| |_|\___\__,_|___/_|\_\    \/
```

## Description

A distributed Key/Value storage based on Bitcask storage model, compatible with RESP protocol.

## How To Run

1. clone
  ```bash
  git clone https://github.com/FinnTew/FincasKV.git
  ```
2. mod tidy
  ```bash
  cd FincasKV && go mod tidy
  ```
3. run
  ```bash
  go run cmd/fincas/main.go [--conf confPath]
  ```

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
  - [x] Handle For Redis DataStructure
    - [x] String
    - [x] List
    - [x] Hash
    - [x] Set
    - [x] ZSet
- [ ] Raft
  - [x] FSM
  - [x] Raft Node
  - [x] Command Apply
    - [x] String
    - [x] List
    - [x] Hash
    - [x] Set
    - [x] ZSet
  - [x] Server Handle
    - [x] INIT
    - [x] JOIN
    - [x] INFO
  - [ ] Forward Write Operation To Leader