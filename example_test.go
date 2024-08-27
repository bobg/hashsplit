package hashsplit_test

import (
	"io"

	"github.com/bobg/hashsplit/v2"
)

func ExampleTreeBuilder() {
	var (
		r  io.Reader // Represents the source of some data.
		tb hashsplit.TreeBuilder
	)

	split, errptr := hashsplit.Split(r)
	for chunk, level := range split {
		if err := tb.Add(chunk, level); err != nil {
			panic(err)
		}
	}
	if err := *errptr; err != nil {
		panic(err)
	}
	// Get the root of the tree with tb.Root().
}

func ExampleTreeBuilder_saveAside() {
	var r io.Reader // Represents the source of some data.

	// Represents any function that replaces a chunk with a compact representation of that chunk
	// (like a hash or a lookup key).
	var saveAside func([]byte) ([]byte, error)

	tb := hashsplit.TreeBuilder{
		F: func(node *hashsplit.TreeBuilderNode) (hashsplit.Node, error) {
			for i, chunk := range node.Chunks {
				repr, err := saveAside(chunk)
				if err != nil {
					return nil, err
				}
				node.Chunks[i] = repr
			}
			return node, nil
		},
	}

	split, errptr := hashsplit.Split(r)
	for chunk, level := range split {
		if err := tb.Add(chunk, level); err != nil {
			panic(err)
		}
	}
	if err := *errptr; err != nil {
		panic(err)
	}
	// Get the root of the tree with tb.Root().
}

func ExampleTreeBuilder_fanOut() {
	var (
		r  io.Reader // Represents the source of some data.
		tb hashsplit.TreeBuilder
	)

	split, errptr := hashsplit.Split(r)
	for chunk, level := range split {
		if err := tb.Add(chunk, level/4); err != nil {
			panic(err)
		}
	}
	if err := *errptr; err != nil {
		panic(err)
	}
	// Get the root of the tree with tb.Root().
}
