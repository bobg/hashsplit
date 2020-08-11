package hashsplit

import (
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"go4.org/rollsum"
)

// BenchmarkRollsum measures how long it takes to roll the checksum and compute a new digest.
func BenchmarkRollsum(b *testing.B) {
	var (
		seed        = getSeed()
		src         = rand.NewSource(seed)
		rnd         = rand.New(src)
		rs          = rollsum.New()
		countZeroes = os.Getenv("BENCHMARK_ROLLSUM_COUNT_ZEROES") == "1"
		zeroes      [32]int
	)
	b.Logf("using seed %d", seed)

	buf := make([]byte, b.N)
	rnd.Read(buf[:])

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs.Roll(buf[i])
		digest := rs.Digest()

		// Would like to call b.StopTimer() here but ran into this bug:
		// https://github.com/golang/go/issues/27217.

		// b.StopTimer()
		if countZeroes {
			for i := 0; i < 32; i++ {
				if digest&(1<<i) == 0 {
					zeroes[i]++
				}
			}
		}
		// b.StartTimer()
	}

	if countZeroes {
		b.Logf("with b.N == %d:", b.N)
		for i, z := range zeroes {
			b.Logf("  bit %d is zero %.1f%% of the time", i, float32(100*z)/float32(b.N))
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
