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
  make build && ./bin/fincaskv [--conf confPath] [--port portValue]
  ```

## Raft Consistency Test

### Node1

```bash
┌─[finntew@FinnTew-PC] - [~] - [四 2月 06, 11:59]
└─[$] <> redis-cli -p 8911
127.0.0.1:8911> cluster init node1 127.0.0.1:7000
OK
127.0.0.1:8911> cluster join node2 127.0.0.1:7001
OK
127.0.0.1:8911> set a bc
OK
127.0.0.1:8911> 
```

### Node2

```bash
┌─[finntew@FinnTew-PC] - [~] - [四 2月 06, 11:59]
└─[$] <> redis-cli -p 8912
127.0.0.1:8912> cluster init node2 127.0.0.1:7001
OK
127.0.0.1:8912> cluster info
127.0.0.1:8912> get a
bc
127.0.0.1:8912> 
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
- [x] Raft (Based on hashicorp/raft)
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
  - [x] Forward Write Operation To Leader
