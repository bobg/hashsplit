package cp32

import (
	"encoding/binary"
	"io"
	"os"
	"testing"
)

func BenchmarkHash(b *testing.B) {
	h := New(64)
	b.ResetTimer()
	for range b.N {
		_, _ = h.Write([]byte("hello"))
	}
}

func TestHash(t *testing.T) {
	cases := []struct {
		name string
		want uint32
	}{{
		name: "commonsense", want: 0xfd835584,
	}, {
		name: "empty", want: 0,
	}, {
		name: "one", want: 0x2b4ff429,
	}, {
		name: "short", want: 0x17f0426d,
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := New(64)
			f, err := os.Open("../testdata/" + tc.name)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()

			_, err = io.Copy(h, f)
			if err != nil {
				t.Fatal(err)
			}

			got := h.Sum32()
			if got != tc.want {
				t.Errorf("got 0x%x, want 0x%x", got, tc.want)
			}
		})
	}
}

func FuzzHash(f *testing.F) {
	f.Fuzz(func(t *testing.T, a, b, c, d uint64, pre byte) {
		var inp [32]byte
		binary.Encode(inp[:], binary.LittleEndian, a)
		binary.Encode(inp[8:], binary.LittleEndian, b)
		binary.Encode(inp[16:], binary.LittleEndian, c)
		binary.Encode(inp[24:], binary.LittleEndian, d)

		h1 := New(32)
		h2 := New(32)

		h1.Write(inp[:])
		h2.WriteByte(pre)
		h2.Write(inp[:len(inp)-1])
		h2.WriteByte(inp[len(inp)-1])

		got1 := h1.Sum32()
		got2 := h2.Sum32()

		if got1 != got2 {
			t.Errorf("h1: 0x%x, h2: 0x%x (on input %x/%d)", got1, got2, inp[:], pre)
		}
	})
}
