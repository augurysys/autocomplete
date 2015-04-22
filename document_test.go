package autocomplete

import (
	"reflect"
	"testing"
)

type doc struct {
	id   string
	term string
	data string
}

func (d doc) ID() string {
	return d.id
}

func (d doc) Term() string {
	return d.term
}

func (d doc) Data() interface{} {
	return d.data
}

func TestKey(t *testing.T) {
	d := doc{
		id:   "123",
		term: "Test SEARCH term!",
		data: "dbID123",
	}

	if key(d) != "test_search_term!_123" {
		t.Fail()
	}
}

func TestPrefixes(t *testing.T) {
	d := doc{
		id:   "123",
		term: "Test SEARCH term!",
		data: "dbID123",
	}

	if !reflect.DeepEqual(prefixes(d),
		[]string{"t", "te", "tes", "test", "s", "se", "sea", "sear", "searc",
			"search", "ter", "term", "term!"}) {

		t.Fail()
	}
}

func TestAppendUnique(t *testing.T) {
	slice := []string{"a", "b", "c"}

	if !reflect.DeepEqual(appendUnique(slice, "a"), []string{"a", "b", "c"}) {
		t.Fail()
	}

	if !reflect.DeepEqual(
		appendUnique(slice, "d"), []string{"a", "b", "c", "d"}) {

		t.Fail()
	}
}
