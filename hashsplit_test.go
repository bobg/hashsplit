package hashsplit

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
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
	split, errptr := Split(f)
	for chunk := range split {
		i++
		want, err := os.ReadFile(fmt.Sprintf("testdata/chunk%02d", i))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(chunk, want) {
			t.Errorf("mismatch in chunk %d", i)
		}
	}
	if err := *errptr; err != nil {
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

		split, errptr := Split(bytes.NewReader(inp))
		for chunk := range split {
			got = append(got, chunk...)
		}
		if err := *errptr; err != nil {
			t.Fatal(err)
		}
		if len(got) != num {
			t.Errorf("got %d byte(s), want %d", len(got), num)
		}
	}
}

func TestTree(t *testing.T) {
	text, err := os.ReadFile("testdata/commonsense.txt")
	if err != nil {
		t.Fatal(err)
	}

	root, err := buildTree(text)
	if err != nil {
		t.Fatal(err)
	}

	if !compareTrees(root, wantTree) {
		t.Fatal("tree mismatch")
	}

	var innerErr error

	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		for chunk := range root.AllChunks() {
			_, innerErr = pw.Write(chunk)
			if innerErr != nil {
				return
			}
		}
	}()

	reassembled, err := io.ReadAll(pr)
	if err != nil {
		t.Fatal(err)
	}
	if innerErr != nil {
		t.Fatal(innerErr)
	}
	if !bytes.Equal(text, reassembled) {
		t.Error("reassembled text does not match original")
	}
}

func TestSeek(t *testing.T) {
	text, err := os.ReadFile("testdata/commonsense.txt")
	if err != nil {
		t.Fatal(err)
	}

	root, err := buildTree(text)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name    string
		pos     uint64
		want    *TreeNode
		wanterr bool
	}{{
		name: "left end",
		pos:  0,
		want: &TreeNode{Chunks: [][]byte{nil, nil}, Size: 35796},
	}, {
		name: "right end",
		pos:  31483 + 116651 - 1,
		want: &TreeNode{Chunks: [][]byte{nil, nil, nil}, Size: 31483, Offset: 116651},
	}, {
		name:    "past the end",
		pos:     31483 + 116651,
		want:    nil,
		wanterr: true,
	}, {
		name: "in the middle",
		pos:  100000,
		want: &TreeNode{Chunks: [][]byte{nil}, Size: 6775, Offset: 98993},
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
			if !compareTrees(got, c.want) {
				t.Errorf("got %+v, want %+v", got, c.want)
			}
		})
	}
}

func BenchmarkTree(b *testing.B) {
	text, err := os.ReadFile("testdata/commonsense.txt")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = buildTree(text)
	}
}

func buildTree(text []byte) (*TreeNode, error) {
	split, errptr := Split(bytes.NewReader(text))
	tree := Tree(split)
	var root *TreeNode
	for node := range tree {
		root = node
	}
	return root, *errptr
}

// Compares two trees, disregarding the contents of the leaves.
func compareTrees(a, b *TreeNode) bool {
	if a == nil {
		return b == nil
	}
	if b == nil {
		return false
	}
	if len(a.Children) != len(b.Children) {
		return false
	}
	if len(a.Chunks) != len(b.Chunks) {
		return false
	}
	if a.Size != b.Size {
		return false
	}
	if a.Offset != b.Offset {
		return false
	}

	for i := 0; i < len(a.Children); i++ {
		if !compareTrees(a.Children[i], b.Children[i]) {
			return false
		}
	}

	return true
}

// The shape, but not the leaf content, of the expected tree.
const wantTreeJSON = `
{
  "Children": [
    {
      "Children": [
        {
          "Children": [
            {
              "Children": [
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
          "Children": [
            {
              "Children": [
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
          "Children": [
            {
              "Children": [
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
      "Children": [
        {
          "Children": [
            {
              "Children": [
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
              "Children": [
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
              "Children": [
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

var wantTree *TreeNode

func init() {
	var w TreeNode
	err := json.Unmarshal([]byte(wantTreeJSON), &w)
	if err != nil {
		panic(err)
	}
	wantTree = &w
}
