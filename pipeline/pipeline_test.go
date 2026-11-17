package pipeline

import "testing"

func TestProcessEmptyInput(t *testing.T) {
	results := Process(nil, 4)
	if len(results) != 0 {
		t.Fatalf("len(results) = %d; want 0", len(results))
	}
}
