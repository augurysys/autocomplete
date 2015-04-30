package autocomplete

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"sync"

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
	orderBy int) ([][]byte, error) {

	conn := a.pool.Get()
	defer conn.Close()

	terms := strings.Split(strings.ToLower(query), " ")
	if len(terms) == 0 {
		return [][]byte{}, nil
	}

	var zkey string
	idx := a.prefix + ":$" + index

	if len(terms) == 1 {
		zkey = a.prefix + ":" + index + ":" + terms[0]
	} else {
		buf := bytes.NewBufferString(idx + ":")
		for i, t := range terms {
			buf.WriteString(t)
			if i < len(terms)-1 {
				buf.WriteString("|")
			}
		}

		zkey = buf.String()

		keys := []string{}
		for _, t := range terms {
			keys = append(keys, a.prefix+":"+index+":"+t)
		}

		args := []interface{}{zkey, len(terms)}
		for _, k := range keys {
			args = append(args, k)
		}

		args = append(args, []interface{}{"AGGREGATE", "MAX"}...)
		if _, err := conn.Do("ZINTERSTORE", args...); err != nil {
			return [][]byte{}, err
		}
	}

	var values []interface{}
	var err error

	switch orderBy {
	case SortLexicographical:
		values, err = redis.Values(conn.Do("ZRANGE", zkey, 0, -1))

	case SortRevLexicographical:
		values, err = redis.Values(conn.Do("ZREVRANGE", zkey, 0, -1))

	case SortScore:
		values, err = redis.Values(conn.Do("ZRANGEBYSCORE", zkey, "-inf", "+inf"))

	case SortRevScore:
		values, err = redis.Values(conn.Do("ZREVRANGEBYSCORE", zkey, "+inf", "-inf"))
	}

	if err != nil {
		return [][]byte{}, err
	}

	keys := []string{}
	for _, r := range values {
		b, ok := r.([]byte)
		if !ok {
			return [][]byte{}, fmt.Errorf("type assertion error")
		}

		keys = append(keys, string(b))
	}

	if orderBy == SortLexicographical {
		sort.Sort(sort.StringSlice(keys))
	} else if orderBy == SortRevLexicographical {
		sort.Sort(sort.Reverse(sort.StringSlice(keys)))
	}

	results := [][]byte{}
	queries := make([][]string, int(len(keys)/1000)+1)
	queryResults := make(map[int][]interface{})

	for i, k := range keys {
		queries[int(i/1000)] = append(queries[int(i/1000)], k)
	}

	var wg sync.WaitGroup
	e := make(chan error, len(queries)+1)

	for i, keys := range queries {
		if len(keys) == 0 {
			continue
		}

		wg.Add(1)
		go func(i int, keys []string) {
			defer wg.Done()

			conn := a.pool.Get()
			defer conn.Close()

			args := []interface{}{idx}
			for _, k := range keys {
				args = append(args, k)
			}

			values, err := redis.Values(conn.Do("HMGET", args...))
			if err != nil {
				e <- err
				return
			}

			queryResults[i] = values
		}(i, keys)
	}

	if len(terms) > 1 {
		go func() {
			conn := a.pool.Get()
			defer conn.Close()

			if err := conn.Send("EXPIRE", zkey, 60); err != nil {
				e <- err
			}
		}()
	}

	wg.Wait()
	if len(e) > 0 {
		return [][]byte{}, <-e
	}

	for _, q := range queryResults {
		for _, v := range q {
			b, ok := v.([]byte)
			if !ok {
				return [][]byte{}, fmt.Errorf("type assertion error")
			}

			results = append(results, b)
		}
	}

	return results, nil
}

func (a *Autocomplete) termsSearch(index, query string,
	orderBy int) ([][]byte, error) {

	conn := a.pool.Get()
	defer conn.Close()

	var values []interface{}
	var err error

	zkey := a.prefix + ":$$" + index
	q := strings.ToLower(query)

	switch orderBy {
	case SortScore:
		fallthrough
	case SortRevScore:
		fallthrough
	case SortLexicographical:
		values, err = redis.Values(conn.Do("ZRANGEBYLEX", zkey, "["+q, "["+q+"\xff"))

	case SortRevLexicographical:
		values, err = redis.Values(conn.Do("ZREVRANGEBYLEX", zkey, "["+q+"\xff", "["+q))
	}

	if err != nil {
		return [][]byte{}, err
	}

	vals := []string{}
	for _, r := range values {
		b, ok := r.([]byte)
		if !ok {
			return [][]byte{}, fmt.Errorf("type assertion error")
		}

		vals = append(vals, string(b))
	}

	if orderBy == SortScore {
		sort.Sort(byScore(vals))
	} else if orderBy == SortRevScore {
		sort.Sort(sort.Reverse(byScore(vals)))
	}

	keys := []string{}
	for _, v := range vals {
		parts := strings.Split(v, "::")
		key := parts[len(parts)-1]
		keys = append(keys, key)
	}

	results := [][]byte{}
	queries := make([][]string, int(len(keys)/1000)+1)
	queryResults := make(map[int][]interface{})

	for i, k := range keys {
		queries[int(i/1000)] = append(queries[int(i/1000)], k)
	}

	var wg sync.WaitGroup
	e := make(chan error, len(queries))

	for i, keys := range queries {
		if len(keys) == 0 {
			continue
		}

		wg.Add(1)
		go func(i int, keys []string) {
			defer wg.Done()

			conn := a.pool.Get()
			defer conn.Close()

			args := []interface{}{a.prefix + ":$" + index}
			for _, k := range keys {
				args = append(args, k)
			}

			values, err := redis.Values(conn.Do("HMGET", args...))
			if err != nil {
				e <- err
				return
			}
			queryResults[i] = values
		}(i, keys)
	}

	wg.Wait()
	if len(e) > 0 {
		return [][]byte{}, <-e
	}

	for _, q := range queryResults {
		for _, v := range q {
			b, ok := v.([]byte)
			if !ok {
				return [][]byte{}, fmt.Errorf("type assertion error")
			}

			results = append(results, b)
		}
	}

	return results, nil
}

type byScore []string

func (v byScore) Len() int {
	return len(v)
}

func (v byScore) Less(i, j int) bool {
	partsi := strings.Split(v[i], "::")
	partsj := strings.Split(v[j], "::")

	scorei := partsi[len(partsi)-2]
	scorej := partsj[len(partsj)-2]

	return scorei < scorej
}

func (v byScore) Swap(i, j int) {
	v[i], v[j] = v[j], v[i]
}
