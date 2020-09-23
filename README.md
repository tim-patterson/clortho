# Clortho
![Build Status](https://github.com/tim-patterson/clortho/workflows/Test/badge.svg)

Experiments with cloud native KV storage backend targeting streaming type workloads

## Goal
While I do have big lofty goals the reality is this is a big project that will most likely
never get anywhere close to working.
The eventual goal would be to create a rocksdb type kv store with the following differences:

* Timestamp aware allowing point in time queries - while this can be implemented on top of
any kv store by pushing this down to the storage layer we can be a bit smarter with compactions,
truncating history efficiently, bucketing history for performance where we're only querying recent
history etc.
* Simple sharding leaning on cloud services for atomic commits etc instead of WAL's and replication
* Giving up low latency writes for higher latency cloud commits, each write batch would
consist of whole sst's.
* Support for file level custom stats and hooks for filtering, ie column min/max/blooms etc to allow for application
aware file pruning.
* Potentially different compaction etc for log type data.
* Distributed (with a co-located higher level processing).

This project will attempt to build up the components needed bit by bit hopefully allowing
each bit to be generic enough to be able to stand on its own.


## Project Status
As mentioned above this is project really consists of a collection of building blocks needed to build
a KV(T) stores, as we progress we'll be able to arrange those blocks in different ways to form
different embedded kv stores.

#### Phase 1 - File layers
The goal for the end of this phase is just to have a file format that we can read and write
- [x] FileStore Abstraction and InMem Impl
- [x] SST Writer(v1)
- [ ] SST Reader(v1)
- [ ] Local FileStore Impl

#### Phase 2 - LSM
The goal for this phase is to build the abstractions needed to progress from a bunch of SST's
to a real LSM, at this point we need to define the abstractions that know about deltas vs
absolutes vs tombstones
- [ ] Merge function abstractions
- [ ] Merging Iterator
- [ ] LSM tree (meta) data structure.
- [ ] Compaction Abstraction and Infra
- [ ] LSM api

#### Phase 3 - Bloom
The goal for this phase is to build in the abstractions and infra needed to support
filter files that can be built during sst writes to allow filtering at query time
- [ ] Bloom filter implementation
- [ ] Filter Writer Trait and default implementation
- [ ] Api/hooks to hook this in to LSM api for queries

#### Phase 4 - Cloud
The goal for this phase is to support files being offloaded and retrieved to s3 and
metadata(SST Tree) stored in dynamo.
- [ ] Cloud FileStore implementation
- [ ] Dynamo metadata syncing.

## How(the plan)
The current idea here is to very roughly follow the basic rocksdb structure but with
s3 being the "master" storage with files cached locally with metadata in dynamodb.
Because we can lean on dynamo for our atomic state updates we no longer have a need for
wal files and the complexity that comes with it.

To support clustering we'll use dynamo as distributed lock/leader election and a reasonably high
frequency gossip protocol across the cluster to share state.


## Developing
Before checking in all tests need to pass,
the code needs to be formatted and lints need to pass.
```sh
  cargo test --workspace --tests
  cargo fmt --all
  cargo clippy --all
```
