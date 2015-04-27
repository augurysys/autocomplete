package autocomplete

import (
	"reflect"
	"testing"
)

type doc struct {
	DocID   string `json:"id"`
	Name    string `json:"name"`
	DocData string `json:"data,omitempty"`
}

func (d doc) ID() string {
	return d.DocID
}

func (d doc) Term() string {
	return d.Name
}

func (d doc) Data() interface{} {
	return d.DocData
}

func TestKey(t *testing.T) {
	d := doc{
		DocID:   "123",
		Name:    "Test SEARCH term!",
		DocData: "dbID123",
	}

	if key(d) != "test_search_term!_123" {
		t.Fail()
	}
}

func TestPrefixes(t *testing.T) {
	d := doc{
		DocID:   "123",
		Name:    "Test SEARCH term!",
		DocData: "dbID123",
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
