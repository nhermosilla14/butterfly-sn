package gateway

// maxTopicsPerClient caps the names in one client's table. Ids are 1..0xFFFE and never 0 or 0xFFFF.
const maxTopicsPerClient = 50

// topicTable maps the normal topic names of one session to their ids. Ids are allocated from a counter
// that wraps past 0xFFFE back to 1 and skips ids still in use, so an id freed by remove is reused only
// after the counter comes round to it. Only the session goroutine uses it.
type topicTable struct {
	ids   map[string]uint16
	names map[uint16]string
	last  uint16 // last id allocated; 0 before the first allocation
}

// register returns the id of name, allocating one when name is new. ok is false when the table is full.
func (t *topicTable) register(name string) (id uint16, ok bool) {
	if id, ok := t.ids[name]; ok {
		return id, true
	}
	if len(t.names) >= maxTopicsPerClient {
		return 0, false
	}
	if t.ids == nil {
		t.ids = make(map[string]uint16)
		t.names = make(map[uint16]string)
	}
	id = t.allocID()
	t.ids[name] = id
	t.names[id] = name
	return id, true
}

// allocID returns the next id after last that is not in use. It wraps past 0xFFFE to 1, never returns 0 or
// 0xFFFF, and always finds a free id because the table holds at most maxTopicsPerClient names.
func (t *topicTable) allocID() uint16 {
	for {
		t.last++
		if t.last == 0 || t.last == 0xFFFF {
			t.last = 1
		}
		if _, used := t.names[t.last]; !used {
			return t.last
		}
	}
}

// remove drops name and frees its id for reuse. Removing a name that is not in the table does nothing.
func (t *topicTable) remove(name string) {
	id, ok := t.ids[name]
	if !ok {
		return
	}
	delete(t.ids, name)
	delete(t.names, id)
}

// idOf returns the id registered for name.
func (t *topicTable) idOf(name string) (uint16, bool) {
	id, ok := t.ids[name]
	return id, ok
}

// lookup returns the topic name registered under id.
func (t *topicTable) lookup(id uint16) (string, bool) {
	name, ok := t.names[id]
	return name, ok
}
