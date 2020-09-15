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

	var i int
	err = Split(context.Background(), f, func(chunk []byte, level uint) error {
		i++
		want, err := ioutil.ReadFile(fmt.Sprintf("testdata/chunk%02d", i))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(chunk, want) {
			t.Errorf("mismatch in chunk %d", i)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	const wantChunks = 14
	if i != wantChunks {
		t.Errorf("got %d chunks, want %d", i, wantChunks)
	}
}

func TestTree(t *testing.T) {
	text, err := ioutil.ReadFile("testdata/commonsense.txt")
	if err != nil {
		t.Fatal(err)
	}

	root := buildTree(t, text)

	if len(root.Nodes) != 2 {
		t.Fatalf("want len(root.Nodes)==2, got %d", len(root.Nodes))
	}
	if len(root.Nodes[0].Nodes) != 1 {
		t.Fatalf("want len(root.Nodes[0].Nodes)==1, got %d", len(root.Nodes[0].Nodes))
	}
	if len(root.Nodes[0].Nodes[0].Nodes) != 1 {
		t.Fatalf("want len(root.Nodes[0].Nodes[0].Nodes)==1, got %d", len(root.Nodes[0].Nodes[0].Nodes))
	}
	if len(root.Nodes[0].Nodes[0].Nodes[0].Nodes) != 3 {
		t.Fatalf("want len(root.Nodes[0].Nodes[0].Nodes[0].Nodes)==3, got %d", len(root.Nodes[0].Nodes[0].Nodes[0].Nodes))
	}
	if len(root.Nodes[1].Nodes) != 1 {
		t.Fatalf("want len(root.Nodes[1].Nodes)==1, got %d", len(root.Nodes[1].Nodes))
	}
	if len(root.Nodes[1].Nodes[0].Nodes) != 2 {
		t.Fatalf("want len(root.Nodes[1].Nodes[0].Nodes)==2, got %d", len(root.Nodes[1].Nodes[0].Nodes))
	}
	if len(root.Nodes[1].Nodes[0].Nodes[0].Nodes) != 1 {
		t.Fatalf("want len(root.Nodes[1].Nodes[0].Nodes[0].Nodes)==1, got %d", len(root.Nodes[1].Nodes[0].Nodes[0].Nodes))
	}
	// TODO: Compare the complete tree with what's expected.

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

func BenchmarkTree(b *testing.B) {
	text, err := ioutil.ReadFile("testdata/commonsense.txt")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buildTree(b, text)
	}
}

type fataler interface {
	Fatal(...interface{})
}

func buildTree(f fataler, text []byte) *Node {
	var (
		s  = new(Splitter)
		tb = NewTreeBuilder()
	)
	err := s.Split(context.Background(), bytes.NewReader(text), func(chunk []byte, level uint) error {
		tb.Add(chunk, len(chunk), level)
		return nil
	})
	if err != nil {
		f.Fatal(err)
	}
	return tb.Root()
}
