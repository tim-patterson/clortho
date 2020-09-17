# Cloud Storage
![Build Status](https://github.com/incresql/cloud-storage/workflows/Test/badge.svg)

Experiments with cloud native KV storage backend targeting streaming type workloads

## Goal
To create a true cloud native key value store to support the creation of high performance
cloud native distributed databases that support features that users expect from modern systems.

The databases I have in mind would typically contain these (internal) features:
1. Fast *point in time* indexed lookups/range scans(ie mvcc support).
2. Some form of a log to support incremental updates/streams etc.
3. Fast KV style get/puts for storing state for streaming operators.
4. Cloud native, ie "stateless", distributed and easily scalable(if not autoscaling)
5. A good backup/restore story.
6. Utilises (cheap) cloud storage.
7. Leader election etc for sharded processing/book keeping tasks
8. Soft Realtime writes(ie sub second rather than sub ms)

Commonly the path taken is to have the storage layer be a dumb single node system
with a higher layer dealing with everything distributed.
These systems tend to have alot of complexity to deal with replication, rebalancing,
distributed transactions etc which makes them operationally complex.

This project's point of difference is to:
* Lean on the cloud providers to do all the hard work around replication backups etc
by using cloud storage services as a core component.
* Not be shy about pushing concepts down into the storage layer/file formats if it makes sense
rather than trying to stick to a pure kv store.

## How(the plan)
The current idea here is to very roughly follow the basic rocksdb structure but with
s3 being the primary storage with files cached locally with metadata in dynamodb.
Because we can lean on dynamo for our atomic state updates we no longer have a need for
wal files and the complexity that comes with it, not to mention our atomic updates effectively
become unlimited in size and with very little coordination we can be atomic with many writers
(ie a big distributed query) 


## Developing
Before checking in all tests need to pass,
the code needs to be formatted and lints need to pass.
```sh
  cargo test --workspace --tests
  cargo fmt --all
  cargo clippy --all
```
