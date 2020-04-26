package hashsplit

import (
	"bufio"
	"context"
	"io"
	"math/bits"
)

// Splitter hashsplits its input according to a given RollSum algorithm.
type Splitter struct {
	// E holds any error encountered during Split while reading the input.
	E error

	// RollSum state.
	// Adapted from go4.mod/rollsum
	// (which in turn is adapted from https://github.com/apenwarr/bup,
	// which is adapted from librsync).
	s1, s2         uint32
	window         []byte
	windowSizeBits int
	splitBits      int
	wofs           uint32
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
	s := &Splitter{
		// xxx temporary
		windowSizeBits: 6,
		splitBits:      13,
	}
	s.Reset()
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
	ch := make(chan []byte)

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
					select {
					case <-ctx.Done():
						s.E = ctx.Err()
					case ch <- chunk:
					}
				}
				return
			}
			if err != nil {
				s.E = err
				return
			}
			chunk = append(chunk, c)
			s.Roll(c)
			if s.OnSplit() {
				select {
				case <-ctx.Done():
					s.E = ctx.Err()
					return
				case ch <- chunk:
					chunk = []byte{}
				}
			}
		}
	}()

	return ch
}

func (s *Splitter) Reset() {
	ws := s.windowSize()
	s.s1 = ws * s.charOffset()
	s.s2 = s.s1 * (ws - 1)
	s.window = make([]byte, ws)
	s.wofs = 0
}

func (s *Splitter) Roll(add byte) {
	ws := s.windowSize()
	drop := uint32(s.window[s.wofs])

	s.s1 += uint32(add)
	s.s1 -= drop
	s.s2 += s.s1
	s.s2 -= ws * (drop + s.charOffset())

	s.window[s.wofs] = add
	s.wofs = (s.wofs + 1) & (ws - 1)
}

func (s *Splitter) OnSplit() bool {
	return bits.TrailingZeros32(^s.s2) >= s.splitBits
}

func (s *Splitter) windowSize() uint32 {
	return 1 << s.windowSizeBits
}

func (s *Splitter) charOffset() uint32 {
	return s.windowSize()/2 - 1
}
