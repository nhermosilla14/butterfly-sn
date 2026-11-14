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
	if id, ok := tt.idOf("a/b"); !ok || id != 1 {
		t.Fatalf("idOf(a/b) = %d, %v; want 1", id, ok)
	}
	if _, ok := tt.idOf("never/registered"); ok {
		t.Fatal("idOf of unknown name succeeded")
	}
}

func TestTopicTableFullNeverWraps(t *testing.T) {
	var tt topicTable
	for i := 1; i <= maxTopicsPerClient; i++ {
		id, ok := tt.register(strconv.Itoa(i))
		if !ok || id == 0 || id == 0xFFFF {
			t.Fatalf("allocation %d: id %d, ok %v", i, id, ok)
		}
	}
	if id, ok := tt.register("one more"); ok || id != 0 {
		t.Fatalf("full table: id %d, ok %v; want 0, false", id, ok)
	}
	// A name already in the table still resolves when the table is full.
	if id, ok := tt.register("1"); !ok || id != 1 {
		t.Fatalf("existing name at full table: id %d, ok %v; want 1, true", id, ok)
	}
}

func TestTopicTableNamesUniqueAndNonzero(t *testing.T) {
	var tt topicTable
	seen := make(map[uint16]bool)
	for i := 1; i <= maxTopicsPerClient; i++ {
		id, ok := tt.register("name-" + strconv.Itoa(i))
		if !ok || id == 0 || id == 0xFFFF || seen[id] {
			t.Fatalf("name %d: id %d, ok %v, duplicate %v", i, id, ok, seen[id])
		}
		seen[id] = true
	}
	if len(seen) != maxTopicsPerClient {
		t.Fatalf("got %d distinct ids, want %d", len(seen), maxTopicsPerClient)
	}
}

func TestTopicTableRemoveThenAllocateIsUnique(t *testing.T) {
	var tt topicTable
	a, _ := tt.register("a")
	b, _ := tt.register("b")
	tt.remove("a")
	if _, ok := tt.idOf("a"); ok {
		t.Fatal("removed name still registered")
	}
	if _, ok := tt.lookup(a); ok {
		t.Fatal("removed id still resolves")
	}
	c, ok := tt.register("c")
	if !ok || c == a || c == b || c == 0 {
		t.Fatalf("id %d after removal: ok %v; want an id differing from %d and %d", c, ok, a, b)
	}
	if name, _ := tt.lookup(b); name != "b" {
		t.Fatalf("lookup(%d) = %q after removal, want b", b, name)
	}
}

func TestTopicTableRemoveUnknownIsNoOp(t *testing.T) {
	var tt topicTable
	tt.remove("never")
	id, _ := tt.register("kept")
	tt.remove("never")
	if got, ok := tt.idOf("kept"); !ok || got != id {
		t.Fatalf("kept: id %d, %v; want %d, true", got, ok, id)
	}
}

func TestTopicTableAllocationWrapsSkippingZeroAndInUse(t *testing.T) {
	var tt topicTable
	tt.last = 0xFFFD
	if id, _ := tt.register("top"); id != 0xFFFE {
		t.Fatalf("top id %#x, want 0xFFFE", id)
	}
	if id, _ := tt.register("wrapped"); id != 1 {
		t.Fatalf("wrapped id %#x, want 1", id)
	}
	// Counter at 0xFFFE: the wrap lands on 1, which is in use, so the next free id is 2.
	tt.last = 0xFFFE
	if id, _ := tt.register("skip"); id != 2 {
		t.Fatalf("skip id %#x, want 2 (1 is in use)", id)
	}
	if id, _ := tt.register("skipped"); id != 3 {
		t.Fatalf("id %#x, want 3", id)
	}
}
