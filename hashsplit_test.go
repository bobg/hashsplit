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

func TestSplitFew(t *testing.T) {
	for num := 0; num < 2; num++ {
		var (
			inp = make([]byte, num)
			got []byte
		)
		err := Split(bytes.NewReader(inp), func(chunk []byte, level uint) error {
			got = append(got, chunk...)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != num {
			t.Errorf("got %d byte(s), want %d", len(got), num)
		}
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
				for _, chunk := range node.Chunks {
					pw.Write(chunk)
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
		name    string
		pos     uint64
		want    *TreeBuilderNode
		wanterr bool
	}{{
		name: "left end",
		pos:  0,
		want: &TreeBuilderNode{Chunks: [][]byte{nil, nil}, size: 35796},
	}, {
		name: "right end",
		pos:  31483 + 116651 - 1,
		want: &TreeBuilderNode{Chunks: [][]byte{nil, nil, nil}, size: 31483, offset: 116651},
	}, {
		name:    "past the end",
		pos:     31483 + 116651,
		want:    nil,
		wanterr: true,
	}, {
		name: "in the middle",
		pos:  100000,
		want: &TreeBuilderNode{Chunks: [][]byte{nil}, size: 6775, offset: 98993},
	}}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := Seek(root, c.pos)
			if c.wanterr {
				if err == nil {
					t.Error("wanted an error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
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

func TestTreeTransform(t *testing.T) {
	text, err := ioutil.ReadFile("testdata/commonsense.txt")
	if err != nil {
		t.Fatal(err)
	}

	tb := TreeBuilder{
		F: func(n *TreeBuilderNode) (Node, error) {
			return &testNode{nodes: n.Nodes, chunks: n.Chunks, size: n.size, offset: n.offset}, nil
		},
	}

	err = Split(bytes.NewReader(text), tb.Add)
	if err != nil {
		t.Fatal(err)
	}

	root, err := tb.Root()
	if err != nil {
		t.Fatal(err)
	}

	var got bytes.Buffer
	err = root.(*testNode).writeto(&got)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(got.Bytes(), text) {
		t.Error("mismatch")
	}
}

type testNode struct {
	nodes        []Node
	chunks       [][]byte
	size, offset uint64
}

func (n *testNode) Offset() uint64              { return n.offset }
func (n *testNode) Size() uint64                { return n.size }
func (n *testNode) NumChildren() int            { return len(n.nodes) }
func (n *testNode) Child(idx int) (Node, error) { return n.nodes[idx], nil }

func (n *testNode) writeto(w io.Writer) error {
	for _, subnode := range n.nodes {
		err := subnode.(*testNode).writeto(w)
		if err != nil {
			return err
		}
	}
	for _, chunk := range n.chunks {
		_, err := w.Write(chunk)
		if err != nil {
			return err
		}
	}
	return nil
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
		tb.Add(chunk, level)
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
	if len(a.Chunks) != len(b.Chunks) {
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
	Chunks       [][]byte
	Size, Offset uint64
}

func (j jsonTBNode) toTBNode() *TreeBuilderNode {
	result := &TreeBuilderNode{
		Chunks: j.Chunks,
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
                  "Chunks": ["", ""],
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
                  "Chunks": ["", "", ""],
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
                  "Chunks": ["", ""],
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
                  "Chunks": [""],
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
                  "Chunks": [""],
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
                  "Chunks": [""],
                  "Size": 557,
                  "Offset": 105768
                },
                {
                  "Chunks": ["", "", ""],
                  "Size": 10326,
                  "Offset": 106325
                },
                {
                  "Chunks": ["", "", ""],
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
