# Decisions

A place to record the thoughts/maths behind technical decisions.
Note none of the performance optimisations have been implemented yet.

### Byte encodings in data blocks
Within the data blocks there are several places we need to record internal pointers or sizes.
To support ok sized files the pointers need to be u32 sized and the record sizes really need
to be at least u24's.

We can reduce the space (and network io) needed by using tricks such as varint encoding at the cost of code
complexity and potentially cpu.

The numbers, assuming 100mb worth of 50byte records, internal 15/16 b+tree pages
```
We'd have:
2,000,000 full records.
125,000 leaf pages(assuming we have the concept of a leaf page)
7,813 lvl 1 pages
489 lvl 2 pages
31 lvl 3 pages
2 lvl 4 pages
1 root page

Thats 8,336 internal nodes containing
125,040 pivots and 133,376 pointers

For each record we also need(naive):
3 bytes key length
1 bytes value length
8 bytes timestamp.
8 bytes frequency.

And the space used by the internal pages would be
for each pivot
  3 bytes key len
  8 bytes timestamp
  4 bytes pivot idx pointer
 50 bytes payload
for each pointer
  4 bytes
for each page
  1 bytes size.

This means to naively store our 100mb of data we'd have
40mb of overhead in the leafs
8.1mb + 0.5mb + 8k = 8.6mb for the internal pages.

Using varint encoding and dropping timestamp to 6 bytes our leaf overheads become
1 bytes key length
1 bytes value length
6 bytes timestamp.
1 bytes frequency.

that reduces the overhead to
18mb for the leafs
8.1mb total for the internal pages.

The two major consumers of this space are still the timestamps and the pivots themselves.
In the pivots we would very rarely need the full payload/timestamp, it just has to be one
byte longer than the common prefix of the left and the right, this should easily half or
quarter the space required to 2-4mb of internal page overhead.

Doing better for timestamps really relys on data naturally clustering.
However if we take a step back we realize that the timestamps are only there to support
point in time queries, at a certain point(minutes/days) after that timestamp we can
effectively set it to zero, if we set a flag at the block level this means timestamps
would consume 0 bytes.

This would take our overheads down to 6mb for the leaves after compaction has zeroed the
timestamps.
```

The conclusion here is that while every byte matters at the record level there's just not
much to be gained space wise by trying to optimize the pointers in the internal pages.


### Prefix compression
There's two semi related but orthogonal concerns here.
1. The space used by repeated keys in our prefixes in the data, if we eliminate this we gain a certain
amount of "cheap compression"
2. The overhead of the comparison functions comparing common keys when walking the tree.

Removing common prefixes in the internal pages is just a complexity vs performance tradeoff.
The implementation should be easy enough that this is a no-brainer.

Removing common prefixes in the data is a space and performance vs complexity tradeoff.
By using prefix compression we also lose the zero-copy nature of being able to serve data
directly out of the mmapped file.

Our implementation would have full keys every 16 records.
We'd full our buffer and then copy over the suffix for every record.




