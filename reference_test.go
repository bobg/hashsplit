package hashsplit_test

// This file tests the test cases from the standard's conformance test suite:
//
// https://github.com/hashsplit/test-suite
//
// We include it as a git submodule under testdata/; make sure you've
// pulled in submodules before running these tests.

import (
	"encoding/json"
	"fmt"
	"github.com/bobg/hashsplit"
	"os"
	"testing"
)

// Data structures used in the test suite's configs.json.
type (
	TestInfo struct {
		Cases map[string]Case `json:"cases"`
	}

	Case struct {
		Config Config `json:"config"`
		Sizes  []int  `json:"sizes"`
	}

	Config struct {
		MinSize   int    `json:"minSize"`
		MaxSize   int    `json:"maxSize"`
		Hash      string `json:"hash"`
		Threshold uint   `json:"threshold"`
	}
)

type TestData struct {
	Name     string
	TestInfo *TestInfo
	Input    []byte
}

func TestReference(t *testing.T) {
	dents, err := os.ReadDir("testdata/test-suite/tests")
	if err != nil {
		t.Fatalf("Error finding test data: %v", err)
	}
	for _, dent := range dents {
		name := dent.Name()
		data, err := loadTestData(name)
		if err != nil {
			t.Errorf("Error loading test data for %q: %v", name, err)
		}
		data.Run(t)
	}
}

func (td *TestData) Run(t *testing.T) {
	t.Logf("Running test cases %q", td.Name)

	for k, v := range td.TestInfo.Cases {
		t.Logf("Testing config %q", k)
		var chunks [][]byte
		s := hashsplit.NewSplitter(func(chunk []byte, level uint) error {
			chunks = append(chunks, chunk)
			return nil
		})
		s.MinSize = v.Config.MinSize
		s.SplitBits = v.Config.Threshold
		// TODO: use MaxSize?

		if v.Config.Hash != "cp32" {
			// We don't support any other algorithms.
			continue
		}

		_, err := s.Write(td.Input)
		if err != nil {
			t.Logf("Error writing to splitter: %v", err)
			t.Fail()
			continue
		}
		err = s.Close()
		if err != nil {
			t.Logf("Error closing splitter: %v", err)
			t.Fail()
			continue
		}
		if len(chunks) != len(v.Sizes) {
			t.Logf("Incorrect number of chunks; wanted %v but got %v",
				len(v.Sizes),
				len(chunks))
			t.Fail()
			continue
		}
		for i := range chunks {
			if len(chunks[i]) != v.Sizes[i] {
				t.Logf("Chunk #%v is the wrong length (wanted %v but got %v)",
					i,
					v.Sizes[i],
					len(chunks[i]))
				t.Fail()
			}
		}
	}
}

func loadTestData(name string) (*TestData, error) {
	testDir := "testdata/test-suite/tests/" + name
	input, err := os.ReadFile(testDir + "/input")
	if err != nil {
		return nil, fmt.Errorf("Error reading input file: %w", err)
	}
	configsJson, err := os.ReadFile(testDir + "/configs.json")
	if err != nil {
		return nil, fmt.Errorf("Error reading configs.json: %w", err)
	}

	info := TestInfo{}
	err = json.Unmarshal(configsJson, &info)
	if err != nil {
		return nil, fmt.Errorf("Error decoding configs.json: %w", err)
	}

	return &TestData{
		Name:     name,
		TestInfo: &info,
		Input:    input,
	}, nil
}
