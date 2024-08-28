package hashsplit_test

import (
	"io"

	"github.com/bobg/seqs"

	"github.com/bobg/hashsplit/v2"
)

func ExampleTree() {
	var (
		r    io.Reader // Represents the source of some data.
		root *hashsplit.TreeNode
	)

	split, errptr := hashsplit.Split(r)
	tree := hashsplit.Tree(split)
	for node := range tree {
		root = node
	}
	if err := *errptr; err != nil {
		panic(err)
	}

	_ = root

	// Now root is the root of the tree.
}

func ExampleTreeBuilder_saveAside() {
	var r io.Reader // Represents the source of some data.

	split, errptr := hashsplit.Split(r)
	tree := hashsplit.Tree(split)

	var (
		// Represents a function that saves a chunk aside and returns a compact representation of it
		// (like a hash or a lookup key).
		saveAside func([]byte) ([]byte, error)

		root *hashsplit.TreeNode
	)

	for node := range tree {
		for i, chunk := range node.Chunks {
			saved, err := saveAside(chunk)
			if err != nil {
				panic(err)
			}
			node.Chunks[i] = saved
		}
		root = node
	}

	if err := *errptr; err != nil {
		panic(err)
	}

	_ = root

	// Now root is the root of the tree.
	// The chunks in the tree have been replaced by lookup keys or whatever.
}

func ExampleTreeBuilder_fanOut() {
	var r io.Reader // Represents the source of some data.

	split, errptr := hashsplit.Split(r)

	var (
		reducedLevelSplit = seqs.Map2(split, func(chunk []byte, level int) ([]byte, int) { return chunk, level / 4 })
		tree              = hashsplit.Tree(reducedLevelSplit)
		root              *hashsplit.TreeNode
	)
	for node := range tree {
		root = node
	}

	if err := *errptr; err != nil {
		panic(err)
	}

	_ = root

	// Now root is the root of the tree, with a wider fanout.
}
