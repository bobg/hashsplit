// Package hashsplit implements content-based splitting of byte streams.
package hashsplit

import (
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

	// The function to invoke on each chunk produced during ReadFrom or Write.
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
	rs := buzhash32.New()
	var zeroes [windowSize]byte
	rs.Write(zeroes[:]) // initialize the rolling checksum window

	return &Splitter{
		f:  f,
		rs: rs,
	}
}

// Write implements io.Writer.
// May produce one or more calls to the callback in s,
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

// Node is a node in the tree built by a TreeBuilder.
// A interior node ("level 1" and higher) contains one or more subnodes as children.
// A leaf node ("level 0") contains one or more byte slices,
// which are normally hashsplit chunks of the input.
// Exactly one of Nodes and Leaves is non-empty.
type Node struct {
	Nodes  []*Node
	Leaves [][]byte

	// Size is the number of bytes represented by this tree node.
	// For a level-0 node this is normally the lengths of the byte slices in Leaves, added together.
	// However, for some applications those byte slices are placeholders for the original data
	// (such as when the original data is saved aside to separate storage).
	// In those cases Size represents the original data, not the placeholder data in Leaves.
	//
	// For higher-level nodes, this is the sum of the Size fields in all child nodes.
	Size uint64

	// Offset is the byte position that this node represents in the original input stream,
	// before splitting.
	// It is equal to the sum of the Size fields of the siblings to this node's "left."
	// Applications can use the Offset field for random access by position to any chunk in the original input stream.
	Offset uint64
}

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
	levels []*Node
}

// NewTreeBuilder produces a new TreeBuilder.
func NewTreeBuilder() *TreeBuilder {
	return &TreeBuilder{levels: []*Node{new(Node)}}
}

// Add adds a new chunk to the TreeBuilder.
// It is typical to call this function in the callback of Split as each chunk is produced.
//
// Normally, size should be len(bytes).
// However, some applications will prefer to save each split chunk aside to separate storage
// rather than place all chunks in the tree.
// In such a case, bytes will be a lookup key for recovering the original chunk,
// and size should be the original size of the chunk (not the size of the lookup key).
// This allows the Size and Offset fields of the nodes in the tree to be correct with respect to the original data.
//
// The level of a chunk is normally the level value passed to the Split callback.
// It results in the creation of a new Node at the given level.
// However, this produces a tree with an average branching factor of 2.
// For wider fan-out (more children per node), the caller can reduce the value of level.
func (tb *TreeBuilder) Add(bytes []byte, size int, level uint) {
	tb.levels[0].Leaves = append(tb.levels[0].Leaves, bytes)
	for _, n := range tb.levels {
		n.Size += uint64(size)
	}
	for i := uint(0); i < level; i++ {
		if i == uint(len(tb.levels))-1 {
			tb.levels = append(tb.levels, &Node{
				Size: tb.levels[i].Size,
			})
		}
		tb.levels[i+1].Nodes = append(tb.levels[i+1].Nodes, tb.levels[i])
		tb.levels[i] = &Node{
			Offset: tb.levels[i+1].Offset + tb.levels[i+1].Size,
		}
	}
}

// Root produces the root of the tree after all nodes have been added with calls to Add.
func (tb *TreeBuilder) Root() *Node {
	if len(tb.levels[0].Leaves) > 0 {
		for i := 0; i < len(tb.levels)-1; i++ {
			tb.levels[i+1].Nodes = append(tb.levels[i+1].Nodes, tb.levels[i])
		}
	}

	root := tb.levels[len(tb.levels)-1]
	for len(root.Nodes) == 1 {
		root = root.Nodes[0]
	}

	return root
}

// Seek finds the level-0 node representing the given byte position
// (i.e., the one where Offset <= pos < Offset+Size).
func Seek(node *Node, pos uint64) *Node {
	if pos < node.Offset || pos >= (node.Offset+node.Size) {
		return nil
	}
	if len(node.Nodes) > 0 {
		for _, subnode := range node.Nodes {
			if n := Seek(subnode, pos); n != nil {
				return n
			}
		}
		return nil // Shouldn't happen
	}
	return node
}
