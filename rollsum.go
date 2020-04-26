package hashsplit

import "math/bits"

// RollSum maintains a rolling checksum of the last N bytes it sees
// (for some value of N).
//
// Adapted from go4.mod/rollsum
// (which in turn is adapted from https://github.com/apenwarr/bup,
// which is adapted from librsync).
type RollSum struct {
	s1, s2         uint32
	window         []byte
	windowSizeBits int
	n              int
	wofs           uint32
}

func NewRollSum() *RollSum {
	rs := &RollSum{
		// xxx temporary
		windowSizeBits: 6,
		n:              13,
	}
	rs.Reset()
	return rs
}

func (rs *RollSum) Reset() {
	ws := rs.windowSize()
	rs.s1 = ws * rs.charOffset()
	rs.s2 = rs.s1 * (ws - 1)
	rs.window = make([]byte, ws)
	rs.wofs = 0
}

func (rs *RollSum) OnSplit() bool {
	return bits.TrailingZeros32(^rs.s2) >= rs.n
}

func (rs *RollSum) Roll(add byte) {
	ws := rs.windowSize()
	drop := uint32(rs.window[rs.wofs])

	rs.s1 += uint32(add)
	rs.s1 -= drop
	rs.s2 += rs.s1
	rs.s2 -= ws * (drop + rs.charOffset())

	rs.window[rs.wofs] = add
	rs.wofs = (rs.wofs + 1) & (ws - 1)
}

func (rs *RollSum) windowSize() uint32 {
	return 1 << rs.windowSizeBits
}

func (rs *RollSum) charOffset() uint32 {
	return rs.windowSize()/2 - 1
}
