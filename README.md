# Clortho
![Build Status](https://github.com/tim-patterson/clortho/workflows/Test/badge.svg)

Experiments with cloud native KV storage backend targeting streaming type workloads

## Goal
While I do have big lofty goals the reality is this is a big project that will most likely
never get anywhere close to working.
The eventual goal would be to create a distributed rocksdb type kv store potentially with
low level support for some of the streaming db type workloads(mvcc style timestamps, time compaction etc)


## Project Status
As mentioned above this is project really consists of a collection of building blocks needed to build
a KV(T) stores, as we progress we'll be able to arrange those blocks in different ways to form
different embedded kv stores.

#### Phase 1 - File layers
The goal for the end of this phase is just to have a file format that we can read and write
- [x] FileStore Abstraction and InMem Impl
- [x] SST Writer(v1)
- [x] SST Reader(v1)
- [x] SST RecordWriter(buffered)
- [x] Local FileStore Impl

#### Phase 2 - LSM
The goal for this phase is to build the abstractions needed to progress from a bunch of SST's
to a real LSM, at this point we need to define the abstractions that know about deltas vs
absolutes vs tombstones
- [ ] Merge function abstractions
- [x] Merging Iterator
- [ ] LSM tree (meta) data structure.
- [ ] Compaction Abstraction and Infra
- [ ] LSM api

#### Phase 3 - Bloom
The goal for this phase is to build in the abstractions and infra needed to support
filter files that can be built during sst writes to allow filtering at query time
- [ ] Bloom filter implementation
- [ ] Filter Writer Trait and default implementation
- [ ] Api/hooks to hook this in to LSM api for queries

#### Phase 4 - Distributed
The goal for this phase is to allow that datastore to be clustered/sharded.



## Developing
Before checking in all tests need to pass,
the code needs to be formatted and lints need to pass.
```sh
  cargo test --workspace --tests
  cargo fmt --all
  cargo clippy --all
```
