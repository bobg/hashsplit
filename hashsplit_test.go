package hashsplit

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/davecgh/go-spew/spew"
)

func TestSplit(t *testing.T) {
	f, err := os.Open("testdata/commonsense.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	ch, errfn := Split(context.Background(), f)
	var i int
	for chunk := range ch {
		i++
		want, err := ioutil.ReadFile(fmt.Sprintf("testdata/chunk%02d", i))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(chunk, want) {
			t.Errorf("mismatch in chunk %d", i)
		}
	}
	const wantChunks = 27
	if i != wantChunks {
		t.Errorf("got %d chunks, want %d", i, wantChunks)
	}
	if err := errfn(); err != nil {
		t.Fatal(err)
	}
}

func TestTree(t *testing.T) {
	f, err := os.Open("testdata/commonsense.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	s := New()
	s.LevelBits = 1

	h := sha256.New()

	s.ChunkFunc = func(b []byte) []byte {
		h.Reset()
		h.Write(b)
		return h.Sum(nil)
	}

	nodes := s.Tree(context.Background(), f)

	var root *Node
	for node := range nodes {
		if root == nil || node.Level > root.Level {
			root = node
		}
	}

	if s.E != nil {
		t.Fatal(s.E)
	}

	fmt.Println(spew.Sdump(root))
}
