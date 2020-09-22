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
