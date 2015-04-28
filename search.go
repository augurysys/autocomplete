package autocomplete

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/garyburd/redigo/redis"
)

// Sort constants
const (
	SortLexicographical    = 0
	SortRevLexicographical = 1
	SortScore              = 2
	SortRevScore           = 3
)

// Search invokes an autocomplete search query
func (a *Autocomplete) Search(index, query string, sort int) ([][]byte, error) {
	switch a.indexType {
	case PrefixesIndexing:
		return a.prefixesSearch(index, query, sort)

	case TermsIndexing:
		return a.termsSearch(index, query, sort)

	default:
		return [][]byte{}, ErrInvalidIndexType
	}
}

func (a *Autocomplete) prefixesSearch(index, query string,
	sort int) ([][]byte, error) {

	conn := a.pool.Get()
	defer conn.Close()

	terms := strings.Split(strings.ToLower(query), " ")
	if len(terms) == 0 {
		return [][]byte{}, nil
	}

	if len(terms) == 1 {
		script, ok := a.scripts["singleTermPrefixSearch"]
		if !ok {
			return [][]byte{}, fmt.Errorf("initialization error")
		}

		values, err := redis.Values(
			script.Do(conn, a.prefix+":"+index+":"+terms[0],
				a.prefix+":$"+index, sort))

		if err != nil {
			return [][]byte{}, err
		}

		results := [][]byte{}
		for _, r := range values {
			b, ok := r.([]byte)
			if !ok {
				return [][]byte{}, fmt.Errorf("type assertion error")
			}

			results = append(results, b)
		}

		return results, nil
	}

	// len(terms) > 1
	buf := bytes.NewBufferString(a.prefix + ":" + index + ":")
	for i, t := range terms {
		buf.WriteString(t)
		if i < len(terms)-1 {
			buf.WriteString("|")
		}
	}

	keys := []string{}
	for _, t := range terms {
		keys = append(keys, a.prefix+":"+index+":"+t)
	}

	args := []interface{}{buf.String(), len(terms)}
	for _, k := range keys {
		args = append(args, k)
	}

	args = append(args, []interface{}{"AGGREGATE", "MAX"}...)
	if _, err := conn.Do("ZINTERSTORE", args...); err != nil {
		return [][]byte{}, err
	}

	script, ok := a.scripts["multiTermPrefixSearch"]
	if !ok {
		return [][]byte{}, fmt.Errorf("initialization error")
	}

	values, err := redis.Values(
		script.Do(conn, buf.String(), a.prefix+":$"+index, sort))

	if err != nil {
		return [][]byte{}, err
	}

	results := [][]byte{}
	for _, r := range values {
		b, ok := r.([]byte)
		if !ok {
			return [][]byte{}, fmt.Errorf("type assertion error")
		}

		results = append(results, b)
	}

	return results, nil
}

func (a *Autocomplete) termsSearch(index, query string,
	sort int) ([][]byte, error) {

	conn := a.pool.Get()
	defer conn.Close()

	script, ok := a.scripts["termSearch"]
	if !ok {
		return [][]byte{}, fmt.Errorf("initialization error")
	}

	values, err := redis.Values(
		script.Do(
			conn,
			a.prefix+":$$"+index,
			a.prefix+":$"+index,
			strings.ToLower(query),
			sort))

	if err != nil {
		return [][]byte{}, err
	}

	results := [][]byte{}
	for _, r := range values {
		b, ok := r.([]byte)
		if !ok {
			return [][]byte{}, fmt.Errorf("type assertion error")
		}

		results = append(results, b)
	}

	return results, nil
}
