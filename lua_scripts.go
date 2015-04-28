package autocomplete

import "github.com/garyburd/redigo/redis"

func (a *Autocomplete) initScripts() {
	a.scripts["removeDocument"] = redis.NewScript(2, `
			local a={}
			local zkey=KEYS[1]
			local key=KEYS[2]
			
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

			a=redis.call("ZRANGE", zkey, 0, -1)
			for i=1,#a do
				local parts=split_func(a[i], "::", 0)
				local akey=parts[table.getn(parts)]
				if akey == key then return a[i] end
			end
			
			return redis.error_reply("key not found in zset")
	`)

	a.scripts["updateScore"] = redis.NewScript(3, `
			local a={}
			local zkey=KEYS[1]
			local key=KEYS[2]
			local val=KEYS[3]
			
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

			local member=""
			a=redis.call("ZRANGE", zkey, 0, -1)
			for i=1,#a do
				local parts=split_func(a[i], "::", 0)
				local akey=parts[table.getn(parts)]
				if akey == key then 
					member=a[i] 
					break
				end
			end
			
			if member == "" then
				return redis.error_reply("key not found in zset")
			end
			
			redis.call("ZREM", zkey, member)
			redis.call("ZADD", zkey, 0, val)
	`)

	a.scripts["isKeyExists"] = redis.NewScript(2, `
			local a={}
			local zkey=KEYS[1]
			local key=KEYS[2]
			
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

			local member=""
			a=redis.call("ZRANGE", zkey, 0, -1)
			for i=1,#a do
				local parts=split_func(a[i], "::", 0)
				local akey=parts[table.getn(parts)]
				if akey == key then 
					return 1
				end
			end
			
			return 0
	`)

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

	a.scripts["singleTermPrefixSearch"] = redis.NewScript(3,
		src+"\n"+"return r")

	a.scripts["multiTermPrefixSearch"] = redis.NewScript(3,
		src+"\n"+`
		redis.call("EXPIRE", zkey, 60)
		return r`)

	a.scripts["termSearch"] = redis.NewScript(4, `
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

		return r
	`)
}
