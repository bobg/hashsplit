package hashsplit

import (
	"bufio"
	"context"
	"io"
	"math/bits"
)

const (
	windowSize uint32 = 64
	charOffset uint32 = 31
)

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
// Hashsplitting is used to dramatically reduce storage and communication requirements
// in projects like git, rsync, bup, and perkeep.
type Splitter struct {
	// Reset says whether to reset the rollsum state to zero at the beginning of each new chunk.
	// The default (what you get when you call New) is false,
	// as in go4.org/rollsum,
	// but that means that a chunk's boundary is determined in part by the chunks that precede it.
	// You probably want to set this to true to make your chunks independent of each other,
	// unless you need go4.org/rollsum-compatible behavior.
	Reset bool

	// MinSize is the minimum chunk size. Only the final chunk may be smaller than this.
	// The default is zero, meaning chunks may be any size. (But they are never empty.)
	MinSize int

	// SplitBits is the number of trailing bits in the rolling checksum that must be set to produce a chunk.
	// The default (what you get when you call New) is 13,
	// which means a chunk boundary occurs on average once every 8,192 bytes.
	//
	// (But thanks to math, that doesn't mean that 8,192 is the median chunk size.
	// The median chunk size is actually the logarithm, base (SplitBits-1)/SplitBits, of 0.5.
	// That makes the median chunk size 5,678 when SplitBits==13.)
	SplitBits int

	// LevelBits is used by Tree to determine when to create new levels of the tree.
	// It is a number of extra trailing bits in the rolling checksum
	// (beyond SplitBits, the number needed to produce a chunk).
	// For example, if SplitBits is 13 and LevelBits is 4 (the default when you call New),
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

	// RollSum state.
	// Adapted from go4.org/rollsum
	// (which in turn is adapted from https://github.com/apenwarr/bup,
	// which is adapted from librsync).
	s1, s2 uint32
	window [windowSize]byte
	wofs   uint32
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
					tz, _ := s.checkSplit()
					var extraBits int
					if tz >= s.SplitBits {
						extraBits = tz - s.SplitBits
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
			s.roll(c)
			if len(chunk) < s.MinSize {
				continue
			}
			if tz, shouldSplit := s.checkSplit(); shouldSplit {
				select {
				case <-ctx.Done():
					s.E = ctx.Err()
					return
				case ch <- chunkPair{bytes: chunk, bits: tz - s.SplitBits}:
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
	s.s1 = windowSize * charOffset
	s.s2 = s.s1 * (windowSize - 1)

	var zeroes [windowSize]byte
	copy(s.window[:], zeroes[:])

	s.wofs = 0
}

func (s *Splitter) roll(add byte) {
	windowSize := windowSize
	drop := uint32(s.window[s.wofs])

	s.s1 += uint32(add)
	s.s1 -= drop
	s.s2 += s.s1
	s.s2 -= windowSize * (drop + charOffset)

	s.window[s.wofs] = add
	s.wofs = (s.wofs + 1) & (windowSize - 1)
}

func (s *Splitter) checkSplit() (int, bool) {
	tz := bits.TrailingZeros32(^s.s2)
	return tz, tz >= s.SplitBits
}

// New produces a new Splitter with the default values of 13 for SplitBits,
// 4 for LevelBits,
// and the identity function for ChunkFunc.
func New() *Splitter {
	s := &Splitter{
		SplitBits: 13,
		LevelBits: 4,
		ChunkFunc: func(b []byte) []byte { return b },
	}
	s.reset()
	return s
}

type Node struct {
	Nodes  []*Node
	Leaves [][]byte
}

func Tree(ctx context.Context, r io.Reader) (*Node, func() error) {
	s := New()
	root := s.Tree(ctx, r)
	return root, func() error { return s.E }
}

func (s *Splitter) Tree(ctx context.Context, r io.Reader) *Node {
	inp := s.split(ctx, r)
	levels := []*Node{new(Node)}

	for chunk := range inp {
		var (
			level = chunk.bits >> s.LevelBits
			b     = s.ChunkFunc(chunk.bytes)
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
			return levels[i]
		}
	}

	return levels[0]
}
