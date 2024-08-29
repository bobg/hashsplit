package hashsplit

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/bobg/seqs"
	"github.com/bradleyjkemp/cupaloy/v2"
	"github.com/google/go-cmp/cmp"
)

func TestSplit(t *testing.T) {
	files, err := os.ReadDir("testdata")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		path := filepath.Join("testdata", file.Name())
		if file.IsDir() {
			continue
		}
		t.Run(file.Name(), func(t *testing.T) {
			text, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			split, errptr := Split(bytes.NewReader(text))
			chunks := slices.Collect(seqs.Left(split))
			if err := *errptr; err != nil {
				t.Fatal(err)
			}

			if len(text) == 0 {
				if len(chunks) != 0 {
					t.Errorf("got %d chunks, want 0", len(chunks))
				}
				return
			}

			snap := cupaloy.New(cupaloy.SnapshotSubdirectory("testdata/snapshots"))
			snap.SnapshotT(t, chunks)

			var got []byte
			for _, chunk := range chunks {
				got = append(got, chunk...)
			}

			if diff := cmp.Diff(string(text), string(got)); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestTree(t *testing.T) {
	files, err := os.ReadDir("testdata")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		path := filepath.Join("testdata", file.Name())
		if file.IsDir() {
			continue
		}
		t.Run(file.Name(), func(t *testing.T) {
			text, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			split, errptr := Split(bytes.NewReader(text))
			tree := Tree(split)
			root, ok := seqs.Last(seqs.Left(tree))
			if err := *errptr; err != nil {
				t.Fatal(err)
			}

			if len(text) == 0 {
				if ok {
					t.Error("got a root node, want an empty tree")
				}
				return
			}

			snap := cupaloy.New(cupaloy.SnapshotSubdirectory("testdata/snapshots"))
			snap.SnapshotT(t, root)

			var got []byte
			for chunk := range root.AllChunks() {
				got = append(got, chunk...)
			}

			if diff := cmp.Diff(string(text), string(got)); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestSeek(t *testing.T) {
	text, err := os.ReadFile("testdata/commonsense")
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
		want: &TreeNode{Chunks: [][]byte{nil}, Size: 1864},
	}, {
		name: "right end",
		pos:  148133,
		want: &TreeNode{Chunks: [][]byte{nil, nil, nil, nil, nil}, Offset: 109169, Size: 38965},
	}, {
		name:    "past the end",
		pos:     200000,
		want:    nil,
		wanterr: true,
	}, {
		name: "in the middle",
		pos:  100000,
		want: &TreeNode{Chunks: [][]byte{nil, nil}, Offset: 92940, Size: 16229},
	}}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := root.Seek(c.pos)
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
	text, err := os.ReadFile("testdata/commonsense")
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
