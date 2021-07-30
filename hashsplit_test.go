package hashsplit

import (
	"bytes"
	"encoding/json"
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
	err = Split(f, func(chunk []byte, level uint) error {
		i++
		if true {
			return ioutil.WriteFile(fmt.Sprintf("testdata/chunk%02d", i), chunk, 0644)
		}

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

	const wantChunks = 16
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

	if !compareTrees(root, wantTree) {
		t.Fatal("tree mismatch")
	}

	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		var walk func(node *TreeBuilderNode)
		walk = func(node *TreeBuilderNode) {
			if len(node.Nodes) > 0 {
				for _, child := range node.Nodes {
					walk(child.(*TreeBuilderNode))
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

func TestSeek(t *testing.T) {
	text, err := ioutil.ReadFile("testdata/commonsense.txt")
	if err != nil {
		t.Fatal(err)
	}

	root := buildTree(t, text)

	cases := []struct {
		name string
		pos  uint64
		want *TreeBuilderNode
	}{
		{"left end", 0, &TreeBuilderNode{Leaves: [][]byte{nil, nil}, size: 35796}},
		{"right end", 31483 + 116651 - 1, &TreeBuilderNode{Leaves: [][]byte{nil, nil, nil}, size: 31483, offset: 116651}},
		{"past the end", 31483 + 116651, nil},
		{"in the middle", 100000, &TreeBuilderNode{Leaves: [][]byte{nil}, size: 6775, offset: 98993}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := Seek(root, c.pos)
			var gottb *TreeBuilderNode
			if got != nil {
				gottb = got.(*TreeBuilderNode)
			}
			if !compareTrees(gottb, c.want) {
				t.Errorf("got %+v, want %+v", gottb, c.want)
			}
		})
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

func buildTree(f fataler, text []byte) *TreeBuilderNode {
	var tb TreeBuilder
	s := NewSplitter(func(chunk []byte, level uint) error {
		tb.Add(chunk, len(chunk), level)
		return nil
	})
	_, err := s.Write(text)
	if err != nil {
		f.Fatal(err)
	}
	err = s.Close()
	if err != nil {
		f.Fatal(err)
	}
	root, err := tb.Root()
	if err != nil {
		f.Fatal(err)
	}
	return root.(*TreeBuilderNode)
}

// Compares two trees, disregarding the contents of the leaves.
func compareTrees(a, b *TreeBuilderNode) bool {
	if a == nil {
		return b == nil
	}
	if b == nil {
		return false
	}
	if len(a.Nodes) != len(b.Nodes) {
		return false
	}
	if len(a.Leaves) != len(b.Leaves) {
		return false
	}
	if a.size != b.size {
		return false
	}
	if a.offset != b.offset {
		return false
	}

	for i := 0; i < len(a.Nodes); i++ {
		if !compareTrees(a.Nodes[i].(*TreeBuilderNode), b.Nodes[i].(*TreeBuilderNode)) {
			return false
		}
	}

	return true
}

type jsonTBNode struct {
	Nodes        []*jsonTBNode
	Leaves       [][]byte
	Size, Offset uint64
}

func (j jsonTBNode) toTBNode() *TreeBuilderNode {
	result := &TreeBuilderNode{
		Leaves: j.Leaves,
		size:   j.Size,
		offset: j.Offset,
	}
	for _, n := range j.Nodes {
		result.Nodes = append(result.Nodes, n.toTBNode())
	}
	return result
}

func (n *TreeBuilderNode) UnmarshalJSON(inp []byte) error {
	var j jsonTBNode
	err := json.Unmarshal(inp, &j)
	if err != nil {
		return err
	}
	*n = *(j.toTBNode())
	return nil
}

// The shape, but not the leaf content, of the expected tree.
const wantTreeJSON = `
{
  "Nodes": [
    {
      "Nodes": [
        {
          "Nodes": [
            {
              "Nodes": [
                {
                  "Leaves": ["", ""],
                  "Size": 35796,
                  "Offset": 0
                }
              ],
              "Size": 35796,
              "Offset": 0
            }
          ],
          "Size": 35796,
          "Offset": 0
        },
        {
          "Nodes": [
            {
              "Nodes": [
                {
                  "Leaves": ["", "", ""],
                  "Size": 38104,
                  "Offset": 35796
                }
              ],
              "Size": 38104,
              "Offset": 35796
            }
          ],
          "Size": 38104,
          "Offset": 35796
        },
        {
          "Nodes": [
            {
              "Nodes": [
                {
                  "Leaves": ["", ""],
                  "Size": 24177,
                  "Offset": 73900
                }
              ],
              "Size": 24177,
              "Offset": 73900
            }
          ],
          "Size": 24177,
          "Offset": 73900
        }
      ],
      "Size": 98077,
      "Offset": 0
    },
    {
      "Nodes": [
        {
          "Nodes": [
            {
              "Nodes": [
                {
                  "Leaves": [""],
                  "Size": 916,
                  "Offset": 98077
                }
              ],
              "Size": 916,
              "Offset": 98077
            },
            {
              "Nodes": [
                {
                  "Leaves": [""],
                  "Size": 6775,
                  "Offset": 98993
                }
              ],
              "Size": 6775,
              "Offset": 98993
            },
            {
              "Nodes": [
                {
                  "Leaves": [""],
                  "Size": 557,
                  "Offset": 105768
                },
                {
                  "Leaves": ["", "", ""],
                  "Size": 10326,
                  "Offset": 106325
                },
                {
                  "Leaves": ["", "", ""],
                  "Size": 31483,
                  "Offset": 116651
                }
              ],
              "Size": 42366,
              "Offset": 105768
            }
          ],
          "Size": 50057,
          "Offset": 98077
        }
      ],
      "Size": 50057,
      "Offset": 98077
    }
  ],
  "Size": 148134,
  "Offset": 0
}`

var wantTree *TreeBuilderNode

func init() {
	var w TreeBuilderNode
	err := json.Unmarshal([]byte(wantTreeJSON), &w)
	if err != nil {
		panic(err)
	}
	wantTree = &w
}
