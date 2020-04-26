package hashsplit

import (
	"bytes"
	"context"
	"fmt"
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

	ch, errfn := Split(context.Background(), f)
	var i int
	for chunk := range ch {
		i++
		want, err := ioutil.ReadFile(fmt.Sprintf("testdata/chunk%02d", i))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(chunk, want) {
			t.Errorf("mismatch in chunk %d", i)
		}
	}
	const wantChunks = 27
	if i != wantChunks {
		t.Errorf("got %d chunks, want %d", i, wantChunks)
	}
	if err := errfn(); err != nil {
		t.Fatal(err)
	}
}
