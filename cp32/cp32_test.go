package cp32

import "testing"

func BenchmarkHash(b *testing.B) {
	h := New(64)
	b.ResetTimer()
	for range b.N {
		h.Write([]byte("hello"))
	}
}
