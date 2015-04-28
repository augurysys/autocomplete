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

	src := `
		local r={}
		local zkey=KEYS[1]
		local index=KEYS[2]
		local sort=tonumber(KEYS[3])
		
		local a={}
		if sort == 0 then
			a=redis.call("ZRANGE", zkey, 0, -1)
			local sort_func=function(a, b) return a < b end
			table.sort(a, sort_func)
		elseif sort == 1 then
			a=redis.call("ZREVRANGE", zkey, 0, -1)
			local sort_func=function(a, b) return a > b end
			table.sort(a, sort_func)
		elseif sort == 2 then
			a=redis.call("ZRANGEBYSCORE", zkey, "-inf", "+inf")
		elseif sort == 3 then
			a=redis.call("ZREVRANGEBYSCORE", zkey, "+inf", "-inf")
		else
			return redis.error_reply("invalid sort value")
		end
		
		for i=1,#a do r[i]=redis.call("HGET", index, a[i]) end`

	if len(terms) == 1 {
		src += "\n" + "return r"
		script := redis.NewScript(3, src)

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
	src += "\n" + `
		redis.call("EXPIRE", zkey, 60)
		return r`

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

	script := redis.NewScript(3, src)
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

	src := `
		local r={}
		local zkey=KEYS[1]
		local index=KEYS[2]
		local query=KEYS[3]
		local sort=tonumber(KEYS[4])
		
		local split_func=function(str, delim, maxNb)
			-- Eliminate bad cases...
		    if string.find(str, delim) == nil then
		        return { str }
		    end
		    if maxNb == nil or maxNb < 1 then
		        maxNb = 0    -- No limit
		    end
		    local result = {}
		    local pat = "(.-)" .. delim .. "()"
		    local nb = 0
		    local lastPos
		    for part, pos in string.gfind(str, pat) do
		        nb = nb + 1
		        result[nb] = part
		        lastPos = pos
		        if nb == maxNb then break end
		    end
		    -- Handle the last field
		    if nb ~= maxNb then
		        result[nb + 1] = string.sub(str, lastPos)
		    end
		    return result
		end
		
		local a={}
		if sort == 0 then
			a=redis.call("ZRANGEBYLEX", zkey, "[" .. query, "[" .. query .. "\xff")
		elseif sort == 1 then
			a=redis.call("ZREVRANGEBYLEX", zkey, "[" .. query .. "\xff", "[" .. query)
		elseif sort == 2 then
			a=redis.call("ZRANGEBYLEX", zkey, "[" .. query, "[" .. query .. "\xff")
			local sort_func=function(a, b)
				local partsa=split_func(a, "::", 0)
				local partsb=split_func(b, "::", 0)
				local scorea=partsa[table.getn(partsa)-1]
				local scoreb=partsb[table.getn(partsb)-1]
				return scorea < scoreb 
			end
			table.sort(a, sort_func)
		elseif sort == 3 then
			a=redis.call("ZRANGEBYLEX", zkey, "[" .. query, "[" .. query .. "\xff")
			local sort_func=function(a, b)
				local partsa=split_func(a, "::", 0)
				local partsb=split_func(b, "::", 0)
				local scorea=partsa[table.getn(partsa)-1]
				local scoreb=partsb[table.getn(partsb)-1]
				return scorea > scoreb 
			end

			table.sort(a, sort_func)
		else
			return redis.error_reply("invalid sort value")
		end
		
		for i=1,#a do 
			local parts=split_func(a[i], "::", 0)
			r[i]=redis.call("HGET", index, parts[table.getn(parts)]) 
		end

		return r`

	script := redis.NewScript(4, src)

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
