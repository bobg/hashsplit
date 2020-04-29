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

type Chunk struct {
	Bytes []byte

	// Bits tells how many extra trailing bits,
	// beyond the SplitBits value needed to create a chunk,
	// were set.
	Bits int
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
func Split(ctx context.Context, r io.Reader) (<-chan Chunk, func() error) {
	s := &Splitter{
		// xxx temporary
		windowSizeBits: 6,
		SplitBits:      13,
	}
	s.reset()
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
func (s *Splitter) Split(ctx context.Context, r io.Reader) <-chan Chunk {
	ch := make(chan Chunk)

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
					case ch <- Chunk{Bytes: chunk, Bits: extraBits}:
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
				case ch <- Chunk{Bytes: chunk, Bits: tz - s.SplitBits}:
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
