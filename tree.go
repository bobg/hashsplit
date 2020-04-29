package hashsplit

type TreeBuilder struct {
	// ChunkFunc maps each incoming chunk to a new byte slice
	// (which can be the original bytes, or a hash of them, or anything else)
	// and a level.
	//
	// On a separate note, saying "ChunkFunc func(Chunk)" out loud is fun.
	ChunkFunc func(Chunk) ([]byte, int)
}

type Node struct {
	Level  int
	Nodes  []*Node
	Leaves [][]byte
}

func (s *TreeBuilder) Tree(inp <-chan Chunk) <-chan *Node {
	out := make(chan *Node)
	levels := []*Node{&Node{Level: 0}}

	go func() {
		defer close(out)
		for chunk := range inp {
			b, level := s.ChunkFunc(chunk)
			levels[0].Leaves = append(levels[0].Leaves, b)
			for i := 0; i < level; i++ {
				if i == len(levels)-1 {
					levels = append(levels, &Node{Level: i + 1})
				}
				levels[i+1].Nodes = append(levels[i+1].Nodes, levels[i])
				out <- levels[i]
				levels[i] = &Node{Level: i}
			}
		}
		if len(levels[0].Leaves) > 0 {
			for i := 0; i < len(levels)-1; i++ {
				levels[i+1].Nodes = append(levels[i+1].Nodes, levels[i])
				out <- levels[i]
			}
			out <- levels[len(levels)-1]
		}
	}()

	return out
}
