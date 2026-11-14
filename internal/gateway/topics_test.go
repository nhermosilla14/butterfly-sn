package gateway

import (
	"strconv"
	"testing"
)

func TestTopicTableIDs(t *testing.T) {
	var tt topicTable
	id, ok := tt.register("a/b")
	if !ok || id != 1 {
		t.Fatalf("first id: %d, %v; want 1", id, ok)
	}
	if again, _ := tt.register("a/b"); again != 1 {
		t.Fatalf("same name: id %d, want 1", again)
	}
	if other, _ := tt.register("A/b"); other != 2 {
		t.Fatalf("case-different name: id %d, want 2 (matching is case-sensitive)", other)
	}
	if name, ok := tt.lookup(2); !ok || name != "A/b" {
		t.Fatalf("lookup(2) = %q, %v", name, ok)
	}
	if _, ok := tt.lookup(3); ok {
		t.Fatal("lookup of unknown id succeeded")
	}
}

func TestTopicTableFullNeverWraps(t *testing.T) {
	var tt topicTable
	for i := 1; i <= maxTopicID; i++ {
		id, ok := tt.register(strconv.Itoa(i))
		if !ok || id == 0 || id == 0xFFFF {
			t.Fatalf("allocation %d: id %d, ok %v", i, id, ok)
		}
	}
	if id, ok := tt.register("one more"); ok || id != 0 {
		t.Fatalf("full table: id %d, ok %v; want 0, false", id, ok)
	}
}
