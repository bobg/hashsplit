// Package hashsplit implements content-based splitting of byte streams.
package hashsplit

import (
	"bufio"
	"errors"
	"io"
	"iter"
	"math/bits"

	"github.com/chmduquesne/rollinghash/buzhash32"
)

const (
	defaultSplitBits = 13
	windowSize       = 64
	defaultMinSize   = windowSize
)

// Splitter hashsplits a byte stream into chunks.
// Create a new Splitter with [NewSplitter].
//
// Hashsplitting is a way of dividing a byte stream into pieces
// based on the stream's content rather than on any predetermined chunk size.
// As the Splitter reads the stream it maintains a rolling checksum of the last several bytes.
// A chunk boundary occurs when the rolling checksum has enough trailing bits set to zero
// (where "enough" is a configurable setting that determines the average chunk size).
//
// Hashsplitting has benefits when it comes to representing multiple,
// slightly different versions of the same data.
// Consider, for example, the problem of adding EXIF tags to a JPEG image file.
// The tags appear near the beginning of the file, and the bulk of the image data follows.
// If the file were divided into chunks at (say) 8-kilobyte boundaries,
// then adding EXIF data near the beginning would alter every following chunk,
// except in the lucky case where the size of the added data is an exact multiple of 8kb.
// With hashsplitting, only the chunks in the vicinity of the change are affected.
//
// Hashsplitting is used to dramatically reduce storage and bandwidth requirements
// in projects like rsync, bup, and perkeep.
type Splitter struct {
	// MinSize is the minimum chunk size.
	// Only the final chunk may be smaller than this.
	// This should always be >= 64,
	// which is the rolling checksum "window size."
	// If it's less than the size of the checksum window,
	// then the same window can span multiple chunks,
	// meaning a chunk boundary is not independent of the preceding chunk.
	// If you leave this set to zero,
	// 64 is what you'll get.
	// If you really mean "I want no minimum,"
	// set this to 1.
	MinSize int

	// SplitBits is the number of trailing zero bits in the rolling checksum required to produce a chunk.
	// The default (what you get if you leave it set to zero) is 13,
	// which means a chunk boundary occurs on average once every 8,192 bytes.
	//
	// (But thanks to math, that doesn't mean that 8,192 is the median chunk size.
	// The median chunk size is actually the logarithm, base (2^SplitBits-1)/(2^SplitBits), of 0.5.
	// That makes the median chunk size 5,678 when SplitBits==13.)
	SplitBits int

	// The chunk being built.
	chunk []byte

	// This is the recommended rolling-checksum algorithm for hashsplitting
	// according to the document at github.com/hashsplit/hashsplit-spec
	// (presently in draft form).
	rs *buzhash32.Buzhash32
}

// Split hashsplits its input using a default Splitter.
// See [Splitter.Split].
func Split(r io.Reader) (iter.Seq2[[]byte, int], *error) {
	s := NewSplitter()
	return s.Split(r)
}

// NewSplitter produces a new Splitter.
// It may be customized before use by setting its MinSize and SplitBits fields.
func NewSplitter() *Splitter {
	rs := buzhash32.New()
	var zeroes [windowSize]byte
	_, _ = rs.Write(zeroes[:]) // initialize the rolling checksum window

	return &Splitter{rs: rs}
}

// Split consumes the given input stream and hashsplits it, producing an iterator of chunk/level pairs.
//
// Each chunk is a subsequence of the input stream.
// Concatenating the chunks in order produces the original stream.
//
// A chunk's "level" is a number that indicates how many trailing zero bits the chunk's hash has,
// in excess of the number required to split the chunk
// (as determined by the Splitter's SplitBits setting).
// You can think of this as a measure of how badly the Splitter wanted to put a chunk boundary here.
// This number is used in constructing a hashsplit tree; see [Tree].
//
// After consuming the returned iterator,
// the caller should dereference the returned error pointer
// to see if any call to the underlying reader produced an error.
//
// The Splitter should not be reused for another stream after this call.
func (s *Splitter) Split(r io.Reader) (iter.Seq2[[]byte, int], *error) {
	var br io.ByteReader
	if b, ok := r.(io.ByteReader); ok {
		br = b
	} else {
		br = bufio.NewReader(r)
	}

	minSize := s.MinSize
	if minSize <= 0 {
		minSize = defaultMinSize
	}

	var err error

	f := func(yield func([]byte, int) bool) {
		for {
			var c byte
			c, err = br.ReadByte()
			if errors.Is(err, io.EOF) {
				err = nil
				if len(s.chunk) > 0 {
					level, _ := s.checkSplit()
					yield(s.chunk, level)
				}
				return
			}
			if err != nil {
				return
			}
			s.chunk = append(s.chunk, c)
			s.rs.Roll(c)
			if len(s.chunk) < minSize {
				continue
			}
			if level, shouldSplit := s.checkSplit(); shouldSplit {
				if !yield(s.chunk, level) {
					return
				}
				s.chunk = nil
			}
		}
	}

	return f, &err
}

func (s *Splitter) checkSplit() (int, bool) {
	splitBits := s.SplitBits
	if splitBits == 0 {
		splitBits = defaultSplitBits
	}
	h := s.rs.Sum32()
	tz := bits.TrailingZeros32(h)
	if tz >= splitBits {
		return tz - splitBits, true
	}
	return 0, false
}

// Tree arranges a sequence of chunks produced by a Splitter into a "hashsplit tree."
// The result is an iterator of TreeNode/level pairs in a particular order;
// see details below.
// The final pair in the sequence is the root of the tree.
//
// A hashsplit tree provides another level of space-and-bandwidth savings
// over and above what Split gives you.
// Consider, again, the example of adding EXIF tags to a JPEG file.
// Although most chunks of the hashsplitted file will be the same before and after adding tags,
// the _list_ needed to reassemble those chunks into the original file will be very different:
// all the unaffected chunks must shift position to accommodate the new EXIF-containing chunks.
//
// A hashsplit tree organizes that list into a tree instead,
// whose shape is determined by the content of the chunks,
// just as the chunk boundaries are.
// It has the property that only the tree nodes in the vicinity of the change will be affected.
// Most subtrees will remain the same.
//
// Just as each chunk has a level L determined by the rolling checksum
// (see [Splitter.Split]),
// so does each node in the tree have a level, N.
// Tree nodes at level 0 collect chunks at level 0,
// up to and including a chunk at level L>0;
// then a new level-0 node begins.
// Tree nodes at level N>0 collect nodes at level N-1
// up to and including a chunk at level L>N;
// then a new level-N node begins.
//
// # ITERATOR DETAILS
//
// Each time Tree consumes a chunk at level L>0,
// it yields the nodes in the partially constructed tree
// that were previously pending,
// from levels 0 through L-1.
//
// After the final chunk is consumed,
// Tree yields the remaining nodes pending, from level 0 up to the root.
//
// For example, Tree may produce a sequence of nodes at these levels:
//
//	0, 1, 0, 1, 2, 0, 1, 0, 1, 2, 3
//
// The level-3 node at the end is the root of the tree.
//
// Callers may discard all but the last node.
// The rest of the tree,
// and the chunks at the leaves,
// are reachable from the root via the Children and Chunks fields.
//
// # TRANSFORMING NODES
//
// The caller may transform nodes as they are produced by the iterator.
// For example, if the input is very large,
// it may not be desirable to keep all the chunks in memory
// (in the leaves of the tree).
// Instead, it might be preferable to "save them aside" to external storage
// and replace them in the tree with some compact representation,
// like a hash or a lookup key.
// Here's how this might be done:
//
//	split, errptr := hashsplit.Split(input)
//	tree := hashsplit.Tree(split)
//	var root *hashsplit.TreeNode
//	for node := range tree {
//	  for i, chunk := range node.Chunks {
//	    saved, err := saveAside(chunk)
//	    if err != nil {
//	      panic(err)
//	    }
//	    node.Chunks[i] = saved
//	  }
//	  root = node
//	}
//	if err := *errptr; err != nil {
//	  panic(err)
//	}
//
// After this, root is the root of the tree,
// and the leaves no longer have the original input chunks,
// but instead have their saved-aside representations.
//
// # FAN-OUT
//
// The chunk levels produced by [Split] result in a tree with an average branching factor of 2
// (i.e., each node has 2 children on average).
// This results in tall trees.
// If you want a wider tree, with more children per node,
// you have to narrow the range of chunk levels seen by Tree.
// Here's how that might look:
//
//	split, errptr := hashsplit.Split(input)
//	reducedLevels := seqs.Map2(split, func(chunk []byte, level int) ([]byte, int) { return chunk, level/4 })
//	tree := hashsplit.Tree(reducedLevels)
//	var root *hashsplit.TreeNode
//	for node := range tree {
//	  root = node
//	}
//	if err := *errptr; err != nil {
//	  panic(err)
//	}
//
// (See https://pkg.go.dev/github.com/bobg/seqs#Map2 for an explanation of the Map2 function used here.)
func Tree(inp iter.Seq2[[]byte, int]) iter.Seq2[*TreeNode, int] {
	return func(yield func(*TreeNode, int) bool) {
		levels := []*TreeNode{{}} // One empty level-0 node.
		for chunk, level := range inp {
			levels[0].Chunks = append(levels[0].Chunks, chunk)
			for _, n := range levels {
				n.Size += uint64(len(chunk))
			}
			for i := 0; i < level; i++ {
				if i == len(levels)-1 {
					levels = append(levels, &TreeNode{
						Size: levels[i].Size,
					})
				}

				n := levels[i]
				levels[i+1].Children = append(levels[i+1].Children, n)

				if !yield(n, i) {
					return
				}

				levels[i] = &TreeNode{
					Offset: levels[i+1].Offset + levels[i+1].Size,
				}
			}
		}

		if len(levels[0].Chunks) == 0 {
			return
		}

		for i := 0; i < len(levels)-1; i++ {
			levels[i+1].Children = append(levels[i+1].Children, levels[i])
		}

		top := len(levels) - 1
		for top > 0 && len(levels[top].Children) == 1 {
			top--
		}
		for i := 0; i <= top; i++ {
			if !yield(levels[i], i) {
				return
			}
		}
	}
}

// Root takes the output of [Tree] and returns the root of the tree.
// It does this by consuming the iterator and returning the last node,
// discarding everything else.
func Root(tree iter.Seq2[*TreeNode, int]) *TreeNode {
	var root *TreeNode
	for node := range tree {
		root = node
	}
	return root
}

// TreeNode is the type of a node in the tree produced by [Tree].
type TreeNode struct {
	// Offset and Size describe the range of the original input stream encompassed by this node.
	Offset, Size uint64

	// Children is the list of subnodes for a node at level N>0.
	// This list is empty for level 0 nodes.
	Children []*TreeNode

	// Chunks is the list of chunks for a node at level 0.
	// This list is empty for nodes at higher levels.
	Chunks [][]byte
}

// AllChunks produces an iterator over all the chunks in the tree.
// It does this with a recursive tree walk starting at n.
func (n *TreeNode) AllChunks() iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		for _, chunk := range n.Chunks {
			if !yield(chunk) {
				return
			}
		}
		for _, child := range n.Children {
			for chunk := range child.AllChunks() {
				if !yield(chunk) {
					return
				}
			}
		}
	}
}

// ErrNotFound is the error returned by Seek when the seek position lies outside the given node's range.
var ErrNotFound = errors.New("not found")

// Seek finds the level-0 node representing the given byte position
// (i.e., the one where Offset <= pos < Offset+Size).
func Seek(n *TreeNode, pos uint64) (*TreeNode, error) {
	if pos < n.Offset || pos >= (n.Offset+n.Size) {
		return nil, ErrNotFound
	}

	num := len(n.Children)
	if num == 0 {
		return n, nil
	}

	// TODO: if a Node kept track of its children's offsets,
	// this loop could be replaced with a sort.Search call.
	for _, child := range n.Children {
		if pos >= (child.Offset + child.Size) {
			continue
		}
		return Seek(child, pos)
	}

	// With a properly formed tree of nodes this will not be reached.
	return nil, ErrNotFound
}
