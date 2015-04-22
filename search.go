package autocomplete

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/garyburd/redigo/redis"
)

// Search invokes an autocomplete search query
func (a *Autocomplete) Search(index, query string) ([][]byte, error) {
	conn := a.pool.Get()
	defer conn.Close()

	terms := strings.Split(strings.ToLower(query), " ")
	if len(terms) == 0 {
		return [][]byte{}, nil
	}

	if len(terms) == 1 {
		script := redis.NewScript(2, `
			local r={}
			local zkey=KEYS[1]
			local index=KEYS[2]
			local a=redis.call("ZRANGE", zkey, 0, -1)
			for i=1,#a do r[i]=redis.call("HGET", index, a[i]) end return r
		`)

		values, err := redis.Values(
			script.Do(conn, a.prefix+":"+index+":"+terms[0], a.prefix+":$"+index))

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

	if _, err := conn.Do("ZINTERSTORE", args...); err != nil {

		return [][]byte{}, err
	}

	script := redis.NewScript(2, `
			local r={}
			local zkey=KEYS[1]
			local index=KEYS[2]
			local a=redis.call("ZRANGE", zkey, 0, -1)
			for i=1,#a do r[i]=redis.call("HGET", index, a[i]) end 
			redis.call("DEL", zkey)
			return r
		`)

	values, err := redis.Values(
		script.Do(conn, buf.String(), a.prefix+":$"+index))

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
