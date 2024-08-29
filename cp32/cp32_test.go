package cp32

import "testing"

func BenchmarkHash(b *testing.B) {
	h := New(64)
	b.ResetTimer()
	for range b.N {
		_, _ = h.Write([]byte("hello"))
	}
}
