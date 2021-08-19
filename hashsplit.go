// Package hashsplit implements content-based splitting of byte streams.
package hashsplit

import (
	"errors"
	"io"
	"math/bits"

	"github.com/chmduquesne/rollinghash/buzhash32"
)

const (
	defaultSplitBits = 13
	windowSize       = 64
	defaultMinSize   = windowSize
)

// Splitter hashsplits a byte sequence into chunks.
// It implements the io.WriteCloser interface.
// Create a new Splitter with NewSplitter.
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
	SplitBits uint

	// The function to invoke on each chunk produced.
	f func([]byte, uint) error

	// The chunk being built.
	chunk []byte

	// This is the recommended rolling-checksum algorithm for hashsplitting
	// according to the document at github.com/hashsplit/hashsplit-spec
	// (presently in draft form).
	rs *buzhash32.Buzhash32
}

// Split hashsplits its input using a default Splitter and the given callback to process chunks.
// See NewSplitter for details about the callback.
func Split(r io.Reader, f func([]byte, uint) error) error {
	s := NewSplitter(f)
	_, err := io.Copy(s, r)
	if err != nil {
		return err
	}
	return s.Close()
}

// NewSplitter produces a new Splitter with the given callback.
// The Splitter is an io.WriteCloser.
// As bytes are written to it,
// it finds chunk boundaries and calls the callback.
//
// The callback receives the bytes of the chunk,
// and the chunk's "level,"
// which is the number of extra trailing zeroes in the rolling checksum
// (in excess of Splitter.SplitBits).
//
// Do not forget to call Close on the Splitter
// to flush any remaining chunk from its internal buffer.
func NewSplitter(f func([]byte, uint) error) *Splitter {
	rs := buzhash32.NewFromUint32Array(cp32G)
	var zeroes [windowSize]byte
	rs.Write(zeroes[:]) // initialize the rolling checksum window

	return &Splitter{f: f, rs: rs}
}

// Write implements io.Writer.
// It may produce one or more calls to the callback in s,
// as chunks are discovered.
// Any error from the callback will cause Write to return early with that error.
func (s *Splitter) Write(inp []byte) (int, error) {
	minSize := s.MinSize
	if minSize <= 0 {
		minSize = defaultMinSize
	}
	for i, c := range inp {
		s.chunk = append(s.chunk, c)
		s.rs.Roll(c)
		if len(s.chunk) < minSize {
			continue
		}
		if level, shouldSplit := s.checkSplit(); shouldSplit {
			err := s.f(s.chunk, level)
			if err != nil {
				return i, err
			}
			s.chunk = nil
		}
	}
	return len(inp), nil
}

// Close implements io.Closer.
// It is necessary to call Close to flush any buffered chunk remaining.
// Calling Close may result in a call to the callback in s.
// It is an error to call Write after a call to Close.
// Close is idempotent:
// it can safely be called multiple times without error
// (and without producing the final chunk multiple times).
func (s *Splitter) Close() error {
	if len(s.chunk) == 0 {
		return nil
	}
	level, _ := s.checkSplit()
	err := s.f(s.chunk, level)
	s.chunk = nil
	return err
}

func (s *Splitter) checkSplit() (uint, bool) {
	splitBits := s.SplitBits
	if splitBits == 0 {
		splitBits = defaultSplitBits
	}
	h := s.rs.Sum32()
	tz := uint(bits.TrailingZeros32(h))
	if tz >= splitBits {
		return tz - splitBits, true
	}
	return 0, false
}

// Node is the abstract type of a node in a hashsplit tree.
// See TreeBuilder for details.
type Node interface {
	// Offset gives the position in the original byte stream that is the first byte represented by this node.
	Offset() uint64

	// Size gives the number of bytes in the original byte stream that this node represents.
	Size() uint64

	// NumChildren gives the number of subnodes of this node.
	// This is only for interior nodes of the tree (level 1 and higher).
	// For leaf nodes (level 0) this must return zero.
	NumChildren() int

	// Child returns the subnode with the given index from 0 through NumChildren()-1.
	Child(int) (Node, error)
}

// TreeBuilderNode is the concrete type implementing the Node interface that is used internally by TreeBuilder.
// Callers may transform this into any other node type during tree construction using the TreeBuilder.F callback.
//
// A interior node ("level 1" and higher) contains one or more subnodes as children.
// A leaf node ("level 0") contains one or more byte slices,
// which are hashsplit chunks of the input.
// Exactly one of Nodes and Chunks is non-empty.
type TreeBuilderNode struct {
	// Nodes is the list of subnodes.
	// This is empty for leaf nodes (level 0) and non-empty for interior nodes (level 1 and higher).
	Nodes []Node

	// Chunks is a list of chunks.
	// This is non-empty for leaf nodes (level 0) and empty for interior nodes (level 1 and higher).
	Chunks [][]byte

	size, offset uint64
}

// Offset implements Node.Offset,
// the position of the first byte of the underlying input represented by this node.
func (n *TreeBuilderNode) Offset() uint64 { return n.offset }

// Size implements Node.Size,
// the number of bytes of the underlying input represented by this node.
func (n *TreeBuilderNode) Size() uint64 { return n.size }

// NumChildren implements Node.NumChildren,
// the number of child nodes.
func (n *TreeBuilderNode) NumChildren() int { return len(n.Nodes) }

// Child implements Node.Child.
func (n *TreeBuilderNode) Child(i int) (Node, error) { return n.Nodes[i], nil }

// TreeBuilder assembles a sequence of chunks into a hashsplit tree.
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
// (see NewSplitter),
// so does each node in the tree have a level, N.
// Tree nodes at level 0 collect chunks at level 0,
// up to and including a chunk at level L>0;
// then a new level-0 node begins.
// Tree nodes at level N>0 collect nodes at level N-1
// up to and including a chunk at level L>N;
// then a new level-N node begins.
type TreeBuilder struct {
	// F is an optional function for transforming the TreeBuilder's node representation
	// (*TreeBuilderNode)
	// into any other type implementing the Node interface.
	// This is called on each node as it is completed and added to its parent as a new child.
	//
	// Callers may wish to perform this transformation when it is not necessary or desirable to keep the full input in memory
	// (i.e., the chunks in the leaf nodes),
	// such as when the input may be very large.
	//
	// F is guaranteed to be called exactly once on each node.
	//
	// If F is nil,
	// all nodes in the tree remain *TreeBuilderNode objects.
	//
	// If this callback return an error,
	// the enclosing function -
	// Add or Root -
	// returns early with that error.
	// In that case the TreeBuilder is left in an inconsistent state
	// and no further calls to Add or Root are possible.
	F func(*TreeBuilderNode) (Node, error)

	levels []*TreeBuilderNode
}

// Add adds a new chunk to the TreeBuilder.
// It is typical to call this function in the callback of Split as each chunk is produced.
//
// The level of a chunk is normally the level value passed to the Split callback.
// It results in the creation of a new node at the given level.
// However, this produces a tree with an average branching factor of 2.
// For wider fan-out (more children per node),
// the caller can reduce the value of level.
func (tb *TreeBuilder) Add(bytes []byte, level uint) error {
	if len(tb.levels) == 0 {
		tb.levels = []*TreeBuilderNode{new(TreeBuilderNode)}
	}
	tb.levels[0].Chunks = append(tb.levels[0].Chunks, bytes)
	for _, n := range tb.levels {
		n.size += uint64(len(bytes))
	}
	for i := uint(0); i < level; i++ {
		if i == uint(len(tb.levels))-1 {
			tb.levels = append(tb.levels, &TreeBuilderNode{
				size: tb.levels[i].size,
			})
		}
		var n Node = tb.levels[i]
		if tb.F != nil {
			var err error
			n, err = tb.F(tb.levels[i])
			if err != nil {
				return err
			}
		}
		tb.levels[i+1].Nodes = append(tb.levels[i+1].Nodes, n)
		tb.levels[i] = &TreeBuilderNode{
			offset: tb.levels[i+1].offset + tb.levels[i+1].size,
		}
	}
	return nil
}

// Root produces the root of the tree after all nodes have been added with calls to Add.
// Root may only be called one time.
// If the tree is empty,
// Root returns a nil Node.
// It is an error to call Add after a call to Root.
//
// The return value of Root is the interface type Node.
// If tb.F is nil, the concrete type will be *TreeBuilderNode.
func (tb *TreeBuilder) Root() (Node, error) {
	if len(tb.levels) == 0 {
		return nil, nil
	}

	if len(tb.levels[0].Chunks) > 0 {
		for i := 0; i < len(tb.levels)-1; i++ {
			var n Node = tb.levels[i]
			if tb.F != nil {
				var err error
				n, err = tb.F(tb.levels[i])
				if err != nil {
					return nil, err
				}
			}
			tb.levels[i+1].Nodes = append(tb.levels[i+1].Nodes, n)
			tb.levels[i] = nil // help the gc reclaim memory sooner, maybe
		}
	}

	// Don't necessarily return the highest node in tb.levels.
	// We can prune any would-be root nodes that have only one child.

	// If we _are_ going to return tb.levels[len(tb.levels)-1],
	// we have to call tb.F on it.
	// If we're not, we don't:
	// tb.F has already been called on all other nodes.

	if len(tb.levels) == 1 {
		var result Node = tb.levels[0]
		if tb.F != nil {
			return tb.F(tb.levels[0])
		}
		return result, nil
	}

	top := tb.levels[len(tb.levels)-1]
	if len(top.Nodes) > 1 {
		if tb.F != nil {
			return tb.F(top)
		}
		return top, nil
	}

	var (
		root Node = top
		err  error
	)
	for root.NumChildren() == 1 {
		root, err = root.Child(0)
		if err != nil {
			return nil, err
		}
	}

	return root, nil
}

// ErrNotFound is the error returned by Seek when the seek position lies outside the given node's range.
var ErrNotFound = errors.New("not found")

// Seek finds the level-0 node representing the given byte position
// (i.e., the one where Offset <= pos < Offset+Size).
func Seek(n Node, pos uint64) (Node, error) {
	if pos < n.Offset() || pos >= (n.Offset()+n.Size()) {
		return nil, ErrNotFound
	}

	num := n.NumChildren()
	if num == 0 {
		return n, nil
	}

	// TODO: if a Node kept track of its children's offsets,
	// this loop could be replaced with a sort.Search call.
	for i := 0; i < num; i++ {
		child, err := n.Child(i)
		if err != nil {
			return nil, err
		}
		if pos >= (child.Offset() + child.Size()) {
			continue
		}
		return Seek(child, pos)
	}

	// With a properly formed tree of nodes this will not be reached.
	return nil, ErrNotFound
}
