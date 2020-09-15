# Cloud Storage
![Build Status](https://github.com/incresql/cloud-storage/workflows/Test/badge.svg)

Experiments with cloud native KV storage backend.

Current distributed databases(ie cassandra, cockroachdb, redshift) work by sharding and replicating data across nodes.
While this model works great and can support high volumes of updates/transactions it does have some downsides.

1. There is a large amount of complexity and hence code to deal with replication and sharding.
2. There is either no support for, or extra complexity required to get atomic updates across shards.
3. Adding/removing nodes is operationally complex and can be slow.
4. Due to the above auto-scaling isn't normally well supported.
5. Storage and compute is tightly coupled.
6. Backups and restores are operationally complex (restores often have to be restored onto the same number of nodes)

To be truly be cloud native these things need to be true:
1. Scaling up/down(even to 0) needs to be quick and painless.
2. Nodes should be stateless(in the sense they don't need to be backed up).
3. Cloud object stores should be utilised for cheap bulk storage
4. We should be able to push the operation concerns of backups etc to the cloud provider

Interestingly this problem is somewhat solved in the batch orientated sql on hadoop world, ie presto/athena.
Here instead of the data being sharded at write time, its simply written hodgepodge to object storage, the "sharding" is instead done during
reads.

Snowflake takes this a couple of steps further, it:
1. Co-locates local caches on the compute nodes to avoid the overhead of going over the network.
2. Uses immutable files and copy on write allowing both crude time travel and lightweight clones for development/testing/staging etc.
These batch orientated solutions do have their own downsides in that for distributed joins and group by's they almost always need
a shuffle.

The goal with this project is to retain most of the properties of a datastore like rocksdb,
that is high performance point lookups, range scans and atomic updates etc.
And to give up low latency updates in exchange for the properties mentioned above.

We can do this by basically creating a hybrid between rocksdb and snowflakes high level arch.


### Developing
Before checking in all tests need to pass,
the code needs to be formatted and lints need to pass.
```sh
  cargo test --workspace --tests
  cargo fmt --all
  cargo clippy --all
```
