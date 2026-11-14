package gateway

// maxTopicID is the largest normal topic id; 0 and 0xFFFF are never issued.
const maxTopicID = 0xFFFE

// topicTable maps the normal topic names of one session to their ids. Ids are never freed, so the
// n-th name gets id n: unique, never 0 and never wrapped. Only the session goroutine uses it.
type topicTable struct {
	ids   map[string]uint16
	names map[uint16]string
}

// register returns the id of name, allocating one when name is new. ok is false when the table is full.
func (t *topicTable) register(name string) (id uint16, ok bool) {
	if id, ok := t.ids[name]; ok {
		return id, true
	}
	if len(t.names) >= maxTopicID {
		return 0, false
	}
	if t.ids == nil {
		t.ids = make(map[string]uint16)
		t.names = make(map[uint16]string)
	}
	id = uint16(len(t.names) + 1)
	t.ids[name] = id
	t.names[id] = name
	return id, true
}

// lookup returns the topic name registered under id.
func (t *topicTable) lookup(id uint16) (string, bool) {
	name, ok := t.names[id]
	return name, ok
}
