package hashsplit

import (
	"bufio"
	"context"
	"io"
	"math/bits"
)

// Splitter hashsplits its input according to a given RollSum algorithm.
type Splitter struct {
	// Reset says whether to reset the rollsum state to zero at the beginning of each new chunk.
	// The default is false (as in go4.org/rollsum),
	// but that means that a chunk's boundary is determined in part by the chunks that precede it.
	Reset bool

	// MinSize is the minimum chunk size. Only the final chunk may be smaller than this.
	// The default is zero, meaning chunks may be any size.
	MinSize int

	// SplitBits is the number of trailing bits in the rolling checksum that must be set to produce a chunk.
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
	LevelBits int

	// ChunkFunc is used by Tree to populate the leaves of the tree.
	// It maps each chunk output from Split to a possibly transformed chunk.
	// A typical use of this function is to replace a chunk with something like its sha256 hash.
	ChunkFunc func([]byte) []byte

	// E holds any error encountered during Split while reading the input.
	// Read it after the channel produced by Split closes.
	E error

	// RollSum state.
	// Adapted from go4.org/rollsum
	// (which in turn is adapted from https://github.com/apenwarr/bup,
	// which is adapted from librsync).
	s1, s2         uint32
	window         []byte
	windowSizeBits int
	wofs           uint32
}

type chunkPair struct {
	bytes []byte
	bits  int
}

// Split hashsplits its input using the rolling checksum from go4.org/rollsum.
// That produces chunks that are mostly between 5kb and 9kb in size.
// For a different size distribution,
// use a Splitter with your own RollSum
// and call its Split method.
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
	s := defaultSplitter()
	ch := s.Split(ctx, r)
	return ch, func() error { return s.E }
}

// Split hashsplits its input using the rolling checksum in s.R.
// Bytes are read from r one at a time and added to the current chunk.
// The current chunk is sent on the output channel when it satisfies RollSum.OnSplit.
// The final chunk may not satisfy OnSplit.
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
	ws := s.windowSize()
	s.s1 = ws * s.charOffset()
	s.s2 = s.s1 * (ws - 1)
	s.window = make([]byte, ws)
	s.wofs = 0
}

func (s *Splitter) roll(add byte) {
	ws := s.windowSize()
	drop := uint32(s.window[s.wofs])

	s.s1 += uint32(add)
	s.s1 -= drop
	s.s2 += s.s1
	s.s2 -= ws * (drop + s.charOffset())

	s.window[s.wofs] = add
	s.wofs = (s.wofs + 1) & (ws - 1)
}

func (s *Splitter) checkSplit() (int, bool) {
	tz := bits.TrailingZeros32(^s.s2)
	return tz, tz >= s.SplitBits
}

func (s *Splitter) windowSize() uint32 {
	return 1 << s.windowSizeBits
}

func (s *Splitter) charOffset() uint32 {
	return s.windowSize()/2 - 1
}

func defaultSplitter() *Splitter {
	s := &Splitter{
		// xxx temporary
		windowSizeBits: 6,
		SplitBits:      13,
		LevelBits:      4,
		ChunkFunc:      func(b []byte) []byte { return b },
	}
	s.reset()
	return s
}

type Node struct {
	Level  int
	Nodes  []*Node
	Leaves [][]byte
}

func Tree(ctx context.Context, r io.Reader) (<-chan *Node, func() error) {
	s := defaultSplitter()
	ch := s.Tree(ctx, r)
	return ch, func() error { return s.E }
}

func (s *Splitter) Tree(ctx context.Context, r io.Reader) <-chan *Node {
	inp := s.split(ctx, r)
	out := make(chan *Node)

	go func() {
		defer close(out)

		var (
			levels = []*Node{new(Node)}
			counts = []int{0}
		)

		for chunk := range inp {
			var (
				level = chunk.bits >> s.LevelBits
				b     = s.ChunkFunc(chunk.bytes)
			)
			levels[0].Leaves = append(levels[0].Leaves, b)
			for i := 0; i < level; i++ {
				if i == len(levels)-1 {
					levels = append(levels, &Node{Level: i + 1})
					counts = append(counts, 0)
				}
				levels[i+1].Nodes = append(levels[i+1].Nodes, levels[i])
				out <- levels[i]
				counts[i]++
				levels[i] = &Node{Level: i}
			}
		}
		if len(levels[0].Leaves) > 0 {
			for i := 0; i < len(levels)-1; i++ {
				levels[i+1].Nodes = append(levels[i+1].Nodes, levels[i])
				out <- levels[i]
				counts[i]++
			}
			out <- levels[len(levels)-1]
		}
	}()

	return out
}
