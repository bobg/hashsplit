package hashsplit

import (
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/chmduquesne/rollinghash/adler32"
	"github.com/chmduquesne/rollinghash/bozo32"
	"github.com/chmduquesne/rollinghash/buzhash32"
	"github.com/chmduquesne/rollinghash/buzhash64"
	"github.com/chmduquesne/rollinghash/rabinkarp64"
	"go4.org/rollsum"
)

type roller interface {
	Roll(byte)
	Digest() uint32
}

// BenchmarkRollsum measures how long it takes to roll the checksum and compute a new digest.
func BenchmarkRollsum(b *testing.B) {
	var (
		seed    = getSeed()
		analyze = os.Getenv("BENCHMARK_ROLLSUM_ANALYZE") == "1"
	)
	b.Logf("using seed %d", seed)

	tests := []struct {
		name   string
		roller roller
	}{
		{
			name:   "rollsum",
			roller: rollsum.New(),
		},
		{
			name:   "adler32",
			roller: newAdler32(64),
		},
		{
			name:   "bozo32",
			roller: newBozo32(64),
		},
		{
			name:   "buzhash32",
			roller: newBuzhash32(64),
		},
		{
			name:   "buzhash64",
			roller: newBuzhash64(64),
		},
		{
			name:   "rabinkarp64",
			roller: newRabinKarp64(64),
		},
	}

	for _, tt := range tests {
		var (
			zeroes       [32]int      // zeroes[i] tells how often bit i is zero
			correlations [32 * 32]int // correlations[32*i+j] tells how often bit i == bit j
			n            int          // records the highest value for b.N
		)

		b.Run(tt.name, func(b *testing.B) {
			n = b.N
			zeroes = [32]int{}
			correlations = [32 * 32]int{}

			var (
				src = rand.NewSource(seed)
				rnd = rand.New(src)
				buf = make([]byte, b.N)
			)
			rnd.Read(buf[:])

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tt.roller.Roll(buf[i])
				digest := tt.roller.Digest()

				// Would like to call b.StopTimer() here but ran into this bug:
				// https://github.com/golang/go/issues/27217.

				// b.StopTimer()
				if analyze {
					for i := 0; i < 32; i++ {
						if digest&(1<<i) == 0 {
							zeroes[i]++
						}
						for j := i + 1; j < 32; j++ {
							if ((digest & (1 << i)) == 0) == ((digest & (1 << j)) == 0) {
								correlations[32*i+j]++
							}
						}
					}
				}
				// b.StartTimer()
			}
		})
		if analyze {
			b.Logf("with b.N == %d:", n)
			for i, z := range zeroes {
				frac := float32(z) / float32(n)
				if frac < .49 || frac > .51 {
					b.Errorf("  bit %d is zero %.1f%% of the time", i, 100.0*frac)
				}
			}
			for i := 0; i < 31; i++ { // sic
				for j := i + 1; j < 32; j++ {
					frac := float32(correlations[32*i+j]) / float32(n)
					if frac < .49 || frac > .51 {
						b.Logf("  bit %d == bit %d %.1f%% of the time", i, j, 100.0*frac)
					}
				}
			}
		}
	}
}

var digest uint32

func getSeed() int64 {
	if s := os.Getenv("BENCHMARK_ROLLSUM_SEED"); s != "" {
		res, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			panic(err)
		}
		return res
	}
	return time.Now().Unix()
}

// adler32

type adler32wrapper struct {
	a *adler32.Adler32
}

func newAdler32(windowSize int) roller {
	w := &adler32wrapper{a: adler32.New()}

	// Objects in the rollinghash module require an initial call to Write to set up their rolling windows.
	z := make([]byte, windowSize)
	w.a.Write(z)
	return w
}

func (w *adler32wrapper) Roll(b byte) {
	w.a.Roll(b)
}

func (w *adler32wrapper) Digest() uint32 {
	return w.a.Sum32()
}

// bozo32

type bozo32wrapper struct {
	b *bozo32.Bozo32
}

func newBozo32(windowSize int) roller {
	w := &bozo32wrapper{b: bozo32.New()}

	// Objects in the rollinghash module require an initial call to Write to set up their rolling windows.
	z := make([]byte, windowSize)
	w.b.Write(z)
	return w
}

func (w *bozo32wrapper) Roll(b byte) {
	w.b.Roll(b)
}

func (w *bozo32wrapper) Digest() uint32 {
	return w.b.Sum32()
}

// buzhash32

type buzhash32wrapper struct {
	b *buzhash32.Buzhash32
}

func newBuzhash32(windowSize int) roller {
	w := &buzhash32wrapper{b: buzhash32.New()}

	// Objects in the rollinghash module require an initial call to Write to set up their rolling windows.
	z := make([]byte, windowSize)
	w.b.Write(z)
	return w
}

func (w *buzhash32wrapper) Roll(b byte) {
	w.b.Roll(b)
}

func (w *buzhash32wrapper) Digest() uint32 {
	return w.b.Sum32()
}

// buzhash64

type buzhash64wrapper struct {
	b *buzhash64.Buzhash64
}

func newBuzhash64(windowSize int) roller {
	w := &buzhash64wrapper{b: buzhash64.New()}

	// Objects in the rollinghash module require an initial call to Write to set up their rolling windows.
	z := make([]byte, windowSize)
	w.b.Write(z)
	return w
}

func (w *buzhash64wrapper) Roll(b byte) {
	w.b.Roll(b)
}

func (w *buzhash64wrapper) Digest() uint32 {
	return uint32(w.b.Sum64())
}

// rabinkarp64

type rabinkarp64wrapper struct {
	r *rabinkarp64.RabinKarp64
}

func newRabinKarp64(windowSize int) roller {
	w := &rabinkarp64wrapper{r: rabinkarp64.New()}

	// Objects in the rollinghash module require an initial call to Write to set up their rolling windows.
	z := make([]byte, windowSize)
	w.r.Write(z)
	return w
}

func (w *rabinkarp64wrapper) Roll(b byte) {
	w.r.Roll(b)
}

func (w *rabinkarp64wrapper) Digest() uint32 {
	return uint32(w.r.Sum64())
}
