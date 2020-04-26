package hashsplit

// RollSum maintains a rolling checksum of the last N bytes it sees
// (for some value of N).
//
// The *rollsum.RollSum type (from go4.org/rollsum) satisfies this interface.
type RollSum interface {
	// OnSplit reports whether the current checksum meets the criteria for a split boundary.
	//
	// (For go4.org/rollsum, this is when the trailing 13 bits are all equal.)
	OnSplit() bool

	// Roll updates the rolling checksum with the value of the latest input byte.
	Roll(byte)
}
