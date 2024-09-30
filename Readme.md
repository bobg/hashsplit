# Hashsplit - content-based splitting of byte streams

[![Go Reference](https://pkg.go.dev/badge/github.com/bobg/hashsplit/v2.svg)](https://pkg.go.dev/github.com/bobg/hashsplit/v2)
[![Go Report Card](https://goreportcard.com/badge/github.com/bobg/hashsplit/v2)](https://goreportcard.com/report/github.com/bobg/hashsplit/v2)
![Tests](https://github.com/bobg/hashsplit/actions/workflows/go.yml/badge.svg)
[![Coverage Status](https://coveralls.io/repos/github/bobg/hashsplit/badge.svg?branch=master)](https://coveralls.io/github/bobg/hashsplit?branch=master)

Hashsplitting is a way of dividing a byte stream into pieces
based on the stream's content rather than on any predetermined chunk size.
As the Splitter reads the stream it maintains a _rolling checksum_ of the last several bytes.
A chunk boundary occurs when the rolling checksum has enough trailing bits set to zero
(where “enough” is a configurable setting that determines the average chunk size).

## Usage

You can split the input from `r`,
an `io.Reader`,
like this:

```go
split, errptr := hashsplit.Split(r)
for chunk := range split {
  // ...handle the contents of r one chunk at a time...
}
if err := *errptr; err != nil {
  // ...handle an error reading from r...
}
```

Chunks can be arranged in a “hashsplit tree” like this:

```go
split, errptr := hashsplit.Split(r)
tree := hashsplit.Tree(split)
root := hashsplit.Root(tree)
if err := *errptr; err != nil {
  // ...handle an error reading from r...
}
```

...and now `root` is the root of a tree whose leaves contain consecutive chunks of the input.

## What is it all about?

Hashsplitting has benefits when it comes to representing multiple,
slightly different versions of the same data.
Consider, for example, the problem of adding EXIF tags to a JPEG image file.
The tags appear near the beginning of the file, and the bulk of the image data follows.
If the file were divided into chunks at (say) 8-kilobyte boundaries,
then adding EXIF data near the beginning would alter every following chunk
(except in the lucky case where the size of the added data is an exact multiple of 8kb).
With hashsplitting, only the chunks in the vicinity of the change are affected.

A sequence of hashsplit chunks can additionally be organized into a tree for even better compaction.
Each chunk has a “level” L determined by the rolling checksum,
and each node in the tree has a level N.
Tree nodes at level 0 collect chunks at level 0,
up to and including a chunk at level L>0;
then a new level-0 node begins.
Tree nodes at level N collect nodes at level N-1
up to and including a chunk at level L>N;
then a new level-N node begins.

Hashsplitting is used to dramatically reduce storage and bandwidth requirements
in projects like
[rsync](https://rsync.samba.org/),
[bup](http://bup.github.io/),
and [perkeep](https://perkeep.org/).
More information,
and a proposed standard,
can be found at
[github.com/hashsplit/hashsplit-spec](https://github.com/hashsplit/hashsplit-spec).
