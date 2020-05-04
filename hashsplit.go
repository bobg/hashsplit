package hashsplit

import (
	"bufio"
	"context"
	"io"
	"math/bits"

	"go4.org/rollsum"
)

const (
	defaultSplitBits = 13
	defaultLevelBits = 4
)

func defaultChunkFunc(b []byte) []byte { return b }

// Splitter hashsplits a byte sequence into chunks.
//
// Hashsplitting is a way of dividing a byte stream into pieces
// based on the stream's content rather than on any predetermined chunk size.
// As the Splitter reads the stream it maintains a rolling checksum of the last several bytes.
// A chunk boundary occurs when the rolling checksum has enough trailing bits set
// (where "enough" is a configurable setting that determines the average chunk size).
//
// Hashsplitting has benefits when it comes to representing multiple,
// slightly different versions of the same data.
// Consider, for example, the problem of adding EXIF tags to a JPEG image file.
// The tags appear near the beginning of the file, and the bulk of the image data follows.
// If the file were divided into chunks at (say) 8-kilobyte boundaries,
// then adding EXIF data near the beginning would alter every following chunk
// (except in the lucky case where the size of the added data is an exact multiple of 8kb).
// With hashsplitting, only the chunks in the vicinity of the change are affected.
//
// Hashsplitting is used to dramatically reduce storage and bandwidth requirements
// in projects like git, rsync, bup, and perkeep.
type Splitter struct {
	// Reset says whether to reset the rollsum state to zero at the beginning of each new chunk.
	// The default is false,
	// as in go4.org/rollsum,
	// but that means that a chunk's boundary is determined in part by the chunks that precede it.
	// You probably want to set this to true to make your chunks independent of each other,
	// unless you need go4.org/rollsum-compatible behavior.
	Reset bool

	// MinSize is the minimum chunk size. Only the final chunk may be smaller than this.
	// The default is zero, meaning chunks may be any size. (But they are never empty.)
	MinSize int

	// SplitBits is the number of trailing bits in the rolling checksum that must be set to produce a chunk.
	// The default (what you get if you leave it set to zero) is 13,
	// which means a chunk boundary occurs on average once every 8,192 bytes.
	//
	// (But thanks to math, that doesn't mean that 8,192 is the median chunk size.
	// The median chunk size is actually the logarithm, base (SplitBits-1)/SplitBits, of 0.5.
	// That makes the median chunk size 5,678 when SplitBits==13.)
	SplitBits int

	// LevelBits is used by Tree to determine when to create new levels of the tree.
	// It is a number of extra trailing bits in the rolling checksum
	// (beyond SplitBits, the number needed to produce a chunk).
	// For example, if SplitBits is 13 and LevelBits is 4,
	// then a new tree level is created after encountering a chunk
	// with 17 trailing rollsum bits set;
	// a second new tree level is created after encountering a chunk
	// with 21 trailing rollsum bits set;
	// and so on.
	//
	// See Tree for more detail.
	LevelBits int

	// ChunkFunc is used by Tree to populate the leaves of the tree.
	// It maps each chunk from Split to a possibly transformed chunk.
	// A typical use of this function is to replace a chunk with something like its sha256 hash.
	// The default simply leaves the chunk unchanged.
	ChunkFunc func([]byte) []byte

	// E holds any error encountered during Split while reading the input.
	// Read it after the channel produced by Split closes.
	E error

	rs *rollsum.RollSum
}

func New() *Splitter {
	return &Splitter{
		SplitBits: defaultSplitBits,
		LevelBits: defaultLevelBits,
		ChunkFunc: defaultChunkFunc,
	}
}

type chunkPair struct {
	bytes []byte
	bits  int
}

// Split hashsplits its input using the default Splitter.
//
// Return values are a channel for the split input chunks,
// and an error function to be called after the channel closes.
//
// Example usage:
//
//   ch, errfn := Split(ctx, r)
//   for chunk := range ch {
//     ...process chunk...
//   }
//   if err := errfn(); err != nil {
//     ...handle error...
//   }
//
// See Splitter.Split for more detail.
func Split(ctx context.Context, r io.Reader) (<-chan []byte, func() error) {
	s := New()
	ch := s.Split(ctx, r)
	return ch, func() error { return s.E }
}

// Split hashsplits its input.
// Bytes are read from r one at a time and added to the current chunk.
// The current chunk is sent on the output channel when s.SplitBits trailing bits of the rollsum state are set.
// The final chunk is sent regardless of the rollsum state, naturally.
//
// After consuming the chunks from the output channel,
// the caller should check s.E to discover whether an error occurred reading from r.
//
// A caller that will not consume all the chunks from the output channel
// should cancel the context object to release resources.
//
// Example usage:
//
//   s := &Splitter{R: new(myRollSum)}
//   ch := s.Split(ctx, r)
//   for chunk := range ch {
//     ...process chunk...
//   }
//   if s.E != nil {
//     ...handle error...
//   }
func (s *Splitter) Split(ctx context.Context, r io.Reader) <-chan []byte {
	chunkPairs := s.split(ctx, r)
	ch := make(chan []byte)
	go func() {
		defer close(ch)
		for chunk := range chunkPairs {
			select {
			case <-ctx.Done():
				return
			case ch <- chunk.bytes:
			}
		}
	}()
	return ch
}

func (s *Splitter) split(ctx context.Context, r io.Reader) <-chan chunkPair {
	ch := make(chan chunkPair)

	splitBits := s.SplitBits
	if splitBits == 0 {
		splitBits = defaultSplitBits
	}

	s.reset()

	go func() {
		defer close(ch)

		var chunk []byte
		rr := bufio.NewReader(r)
		for {
			if err := ctx.Err(); err != nil {
				s.E = err
				return
			}
			c, err := rr.ReadByte()
			if err == io.EOF {
				if len(chunk) > 0 {
					tz, _ := s.checkSplit(splitBits)
					var extraBits int
					if tz >= splitBits {
						extraBits = tz - splitBits
					}
					select {
					case <-ctx.Done():
						s.E = ctx.Err()
					case ch <- chunkPair{bytes: chunk, bits: extraBits}:
					}
				}
				return
			}
			if err != nil {
				s.E = err
				return
			}
			chunk = append(chunk, c)
			s.rs.Roll(c)
			if len(chunk) < s.MinSize {
				continue
			}
			if tz, shouldSplit := s.checkSplit(splitBits); shouldSplit {
				select {
				case <-ctx.Done():
					s.E = ctx.Err()
					return
				case ch <- chunkPair{bytes: chunk, bits: tz - splitBits}:
					chunk = []byte{}
					if s.Reset {
						s.reset()
					}
				}
			}
		}
	}()

	return ch
}

func (s *Splitter) reset() {
	s.E = nil
	s.rs = rollsum.New()
}

func (s *Splitter) checkSplit(splitBits int) (int, bool) {
	h := s.rs.Digest()
	tz := bits.TrailingZeros32(h)
	return tz, tz >= splitBits
}

// Node is a node in the tree returned by Tree.
// A interior node ("level 1" and higher) contains one or more subnodes as children.
// A leaf node ("level 0") contains one or more byte slices,
// which are hashsplit chunks of the input.
// Exactly one of Nodes and Leaves is non-empty.
type Node struct {
	Nodes  []*Node
	Leaves [][]byte
}

// Tree hashsplits its input with the default Splitter
// and assembles the chunks into a hashsplit tree.
//
// Return value is the root of the tree
// (pruned to remove any singleton nodes).
//
// For more detail see Splitter.Tree.
func Tree(ctx context.Context, r io.Reader) (*Node, error) {
	s := New()
	return s.Tree(ctx, r)
}

// Tree hashsplits its input and assembles the chunks into a hashsplit tree.
//
// A hashsplit tree provides another level of space-and-bandwidth savings
// over and above what Split gives you.
// Consider, again, the example of adding EXIF tags to a JPEG file.
// Although most chunks of the file will be the same before and after adding tags,
// the _list_ needed to reassemble those chunks into the original file will be very different:
// all the unaffected chunks must shift position to accommodate the new EXIF-containing chunks.
//
// A hashsplit tree organizes that list into a tree instead,
// with the property that only the tree nodes in the vicinity of the change will be affected.
// Most subtrees will remain the same.
//
// Chunks of hashsplit output are collected in a "level 0" node until
// one whose rolling checksum has s.LevelBits extra bits set
// (beyond s.SplitBits, the number needed to complete a chunk).
// This adds the level-0 node as a child to a new level-1 node.
// If 2*LevelBits extra bits are set,
// that adds the level-1 node to a new level-2 node,
// and so on.
//
// Return value is the root of the tree
// (pruned to remove any singleton nodes).
func (s *Splitter) Tree(ctx context.Context, r io.Reader) (*Node, error) {
	inp := s.split(ctx, r)
	levels := []*Node{new(Node)}

	levelBits := s.LevelBits
	if levelBits == 0 {
		levelBits = defaultLevelBits
	}

	chunkFunc := s.ChunkFunc
	if chunkFunc == nil {
		chunkFunc = defaultChunkFunc
	}

	for chunk := range inp {
		var (
			level = chunk.bits >> levelBits
			b     = chunkFunc(chunk.bytes)
		)
		levels[0].Leaves = append(levels[0].Leaves, b)
		for i := 0; i < level; i++ {
			if i == len(levels)-1 {
				levels = append(levels, new(Node))
			}
			levels[i+1].Nodes = append(levels[i+1].Nodes, levels[i])
			levels[i] = new(Node)
		}
	}
	if len(levels[0].Leaves) > 0 {
		for i := 0; i < len(levels)-1; i++ {
			levels[i+1].Nodes = append(levels[i+1].Nodes, levels[i])
		}
	}

	for i := len(levels) - 1; i > 0; i-- {
		if len(levels[i].Nodes) > 1 {
			return levels[i], s.E
		}
	}

	return levels[0], s.E
}
