package hashsplit

import "io"

func ExampleTreeBuilder() {
	var r io.Reader // Represents the source of some data.

	var tb TreeBuilder
	err := Split(r, func(bytes []byte, level uint) error {
		tb.Add(bytes, len(bytes), level)
		return nil
	})
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

	var tb TreeBuilder
	err := Split(r, func(chunk []byte, level uint) error {
		size := len(chunk)

		// Replace chunk with a compact representation.
		repr, err := saveAside(chunk)
		if err != nil {
			return err
		}

		// Store the compact representation in the tree,
		// not the chunk itself;
		// but use the original chunk's size.
		tb.Add(repr, size, level)
		return nil
	})
	if err != nil {
		panic(err)
	}

	// Get the root of the tree with tb.Root().
}

func ExampleTreeBuilder_fanOut() {
	var r io.Reader // Represents the source of some data.

	var tb TreeBuilder
	err := Split(r, func(bytes []byte, level uint) error {
		// Map level to a smaller range for wider fan-out.
		tb.Add(bytes, len(bytes), level/4)
		return nil
	})
	if err != nil {
		panic(err)
	}

	// Get the root of the tree with tb.Root().
}
