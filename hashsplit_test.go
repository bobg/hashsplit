package hashsplit

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"
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
		if !bytes.Equal(chunk.Bytes, want) {
			t.Errorf("mismatch in chunk %d", i)
		}
	}

	const wantChunks = 18
	if i != wantChunks {
		t.Errorf("got %d chunks, want %d", i, wantChunks)
	}
	if err := errfn(); err != nil {
		t.Fatal(err)
	}
}

func TestTree(t *testing.T) {
	text, err := ioutil.ReadFile("testdata/commonsense.txt")
	if err != nil {
		t.Fatal(err)
	}

	s := new(Splitter)
	ch := s.Split(context.Background(), bytes.NewReader(text))
	ch = Filter(ch, func(chunk Chunk) Chunk {
		chunk2 := chunk
		chunk2.Level /= 2
		return chunk2
	})

	root := Tree(ch)
	if s.E != nil {
		t.Fatal(s.E)
	}

	if len(root.Nodes) != 2 {
		t.Fatalf("want len(root.Nodes)==2, got %d", len(root.Nodes))
	}
	if len(root.Nodes[0].Nodes) != 2 {
		t.Fatalf("want len(root.Nodes[0].Nodes)==2, got %d", len(root.Nodes[0].Nodes))
	}
	if len(root.Nodes[0].Nodes[0].Leaves) != 8 {
		t.Fatalf("want len(root.Nodes[0].Nodes[0].Leaves)==8, got %d", len(root.Nodes[0].Nodes[0].Leaves))
	}
	if len(root.Nodes[0].Nodes[1].Leaves) != 7 {
		t.Fatalf("want len(root.Nodes[0].Nodes[1].Leaves)==7, got %d", len(root.Nodes[0].Nodes[1].Leaves))
	}
	if len(root.Nodes[1].Nodes) != 1 {
		t.Fatalf("want len(root.Nodes[1].Nodes)==1, got %d", len(root.Nodes[1].Nodes))
	}
	if len(root.Nodes[1].Nodes[0].Leaves) != 3 {
		t.Fatalf("want len(root.Nodes[1].Nodes[0].Leaves)==3, got %d", len(root.Nodes[1].Nodes[0].Leaves))
	}

	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		var walk func(node *Node)
		walk = func(node *Node) {
			if len(node.Nodes) > 0 {
				for _, child := range node.Nodes {
					walk(child)
				}
			} else {
				for _, leaf := range node.Leaves {
					pw.Write(leaf)
				}
			}
		}
		walk(root)
	}()

	reassembled, err := ioutil.ReadAll(pr)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(text, reassembled) {
		t.Error("reassembled text does not match original")
	}
}
