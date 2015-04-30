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
}
