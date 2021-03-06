# Block Format

## Data v1
The v1 data blocks are the simplest we can manage.
The data block can roughly be thought of as having the following sections.

```
|header|sorted_data|b+tree pages|footer|
```

We'll default to encoding any fixed sized ints as bigendian..

### Header Section.
The header section doesn't play any important part it the file format,
Most the machine-readable metadata will be at the end of the file.
The header will consist of the 10 textual lines, so we can easily read it
using head, its really just here to help with debugging.
```
clortho
data
v1






---
```

### Sorted Data Section.
The data section is where we store all the records.
Records consist of a Key and a Value.
The data section consists of all the records sorted and laid out as follows:
```
r0:
key_length: varint,
value_length: varint,
key: bytes,
value: bytes,
r1:
key_length: varint,
...
```

However, to stop us walking off the end we'll place `[0,0]`
This could be interpreted as key_length = 0, value_length = 0.
This allows a reader to read the whole record without the early break if
that simplifies any implementations.

### B+Tree Section
This section contains a bunch of btree pages.
The lower pages are first followed by the higher pages.
Each page is laid out as follows
```
pivots: (x's pivot_count)
  key_length: varint
  key_bytes: bytes[key_length]
child_count: u8
pivot_pointers: [u32; child_count - 1]
child_pointers: [i32; child_count]
```

All pointers are absolute.

The branching will be
```
if key < pivot:
  left
else:
  right
```

When thinking about the btree tree remember that each page is a range.
ie. imagine the data section is just a sequence and our pages have 1 pivots each
We end up with a tree like
```
             32
     16              48
0-15,  16-31,  32-47,  48-64
```

ie
`left_child_max < pivot <= right_child_min`

We just use the right_child_min as our pivot but we should be able to trim it down
to 1 longer than the common prefix.

There's couple of twists here:
1. For the child pointers, positives as pointing to other btrees pages
and negatives as pointers into the data section.
2. Due to the var sized pivots coming first in the page, a pointer to a btree will point at the pivot_count, not at the
start of the page.


### Footer Section
Due to us building up the file as we go, our entry point to the file is actually in the footer.
Its main purpose is to provide the initial pointer into our root b+tree page.
Its layout is
```
search_pointer: i32
version: u16(always 1)
```

Just like the pointers in the btree nodes
we'll treat positives as pointing to a btree and a negative as pointing into the data section.
