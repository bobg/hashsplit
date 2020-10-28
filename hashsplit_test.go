package hashsplit

import (
	"bytes"
	"context"
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
	err = Split(context.Background(), f, func(chunk []byte, level uint) error {
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

// Compares two trees, disregarding the contents of the leaves.
func compareTrees(a, b *Node) bool {
	if len(a.Nodes) != len(b.Nodes) {
		return false
	}
	if len(a.Leaves) != len(b.Leaves) {
		return false
	}
	if a.Size != b.Size {
		return false
	}
	if a.Offset != b.Offset {
		return false
	}

	for i := 0; i < len(a.Nodes); i++ {
		if !compareTrees(a.Nodes[i], b.Nodes[i]) {
			return false
		}
	}

	return true
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

var wantTree *Node

func init() {
	var w Node
	err := json.Unmarshal([]byte(wantTreeJSON), &w)
	if err != nil {
		panic(err)
	}
	wantTree = &w
}
