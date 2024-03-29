package hashsplit_test

import (
	"io"

	"github.com/bobg/hashsplit"
)

func ExampleTreeBuilder() {
	var r io.Reader // Represents the source of some data.

	var tb hashsplit.TreeBuilder
	err := hashsplit.Split(r, tb.Add)
	if err != nil {
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
	err := hashsplit.Split(r, tb.Add)
	if err != nil {
		panic(err)
	}
	// Get the root of the tree with tb.Root().
}

func ExampleTreeBuilder_fanOut() {
	var r io.Reader // Represents the source of some data.

	var tb hashsplit.TreeBuilder
	err := hashsplit.Split(r, func(bytes []byte, level uint) error {
		// Map level to a smaller range for wider fan-out
		// (more children per tree node).
		return tb.Add(bytes, level/4)
	})
	if err != nil {
		panic(err)
	}
	// Get the root of the tree with tb.Root().
}
