package autocomplete

import (
	"bytes"
	"strings"
)

// Document is an interface that represents a document which is indexed for
// autocomplete search
type Document interface {
	ID() string
	Term() string
	Data() interface{}
}

func key(d Document) string {
	return strings.Replace(strings.ToLower(d.Term()), " ", "_", -1) + "_" + d.ID()
}

func prefixes(d Document) []string {
	p := []string{}

	terms := strings.Split(d.Term(), " ")
	for _, t := range terms {
		for i := range strings.ToLower(t) {
			buf := bytes.NewBuffer([]byte{})

			for j := 0; j <= i; j++ {
				buf.WriteByte(strings.ToLower(t)[j])
			}

			p = appendUnique(p, buf.String())
		}
	}

	return p
}

func appendUnique(slice []string, s string) []string {
	for _, elem := range slice {
		if elem == s {
			return slice
		}
	}

	return append(slice, s)
}
