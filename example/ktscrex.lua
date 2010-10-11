-- global variables
kt = __kyototycoon__
db = kt.db

-- echo back the input data as the output data
function echo(inmap, outmap)
   for key, value in pairs(inmap) do
      outmap[key] = value
   end
   return kt.RVSUCCESS
end

-- report the internal state of the server
function report(inmap, outmap)
   outmap["__kyototycoon__.VERSION"] = kt.VERSION
   outmap["__kyototycoon__.thid"] = kt.thid
   outmap["__kyototycoon__.db"] = tostring(kt.db)
   for i = 1, #kt.dbs do
      local key = "__kyototycoon__.dbs[" .. i .. "]"
      outmap[key] = tostring(kt.dbs[i])
   end
   local names = ""
   for name, value in pairs(kt.dbs) do
      if #names > 0 then names = names .. "," end
      names = names .. name
   end
   outmap["names"] = names
   return kt.RVSUCCESS
end

-- store a record
function set(inmap, outmap)
   local key = inmap.key
   local value = inmap.value
   if not key or not value then
      return kt.RVEINVALID
   end
   local xt = inmap.xt
   if not db:set(key, value, xt) then
      return kt.RVEINTERNAL
   end
   return kt.RVSUCCESS
end

-- remove a record
function remove(inmap, outmap)
   local key = inmap.key
   if not key then
      return kt.RVEINVALID
   end
   if not db:remove(key) then
      local err = db:error()
      if err:code() == kt.Error.NOREC then
         return kt.RVELOGIC
      end
      return kt.RVEINTERNAL
   end
   return kt.RVSUCCESS
end

-- retrieve the value of a record
function get(inmap, outmap)
   local key = inmap.key
   if not key then
      return kt.RVEINVALID
   end
   local value, xt = db:get(key)
   if value then
      outmap.value = value
      outmap.xt = xt
   else
      local err = db:error()
      if err:code() == kt.Error.NOREC then
         return kt.RVELOGIC
      end
      return kt.RVEINTERNAL
   end
   return kt.RVSUCCESS
end

-- list all records
function list(inmap, outmap)
   local cur = db:cursor()
   cur:jump()
   while true do
      local key, value, xt = cur:get(true)
      if not key then break end
      outmap[key] = value
   end
   return kt.RVSUCCESS
end

-- upcate all characters in the value of a record
function upcase(inmap, outmap)
   local key = inmap.key
   if not key then
      return kt.RVEINVALID
   end
   local function visit(key, value, xt)
      if not value then
         return kt.Visitor.NOP
      end
      print(value)
      return string.upper(value)
   end
   if not db:accept(key, visit) then
      return kt.RVEINTERNAL
   end
   return kt.RVSUCCESS
end

-- prolong the expiration time of a record
function survive(inmap, outmap)
   local key = inmap.key
   if not key then
      return kt.RVEINVALID
   end
   local function visit(key, value, xt)
      if not value then
         return kt.Visitor.NOP
      end
      outmap["old_xt"] = xt
      if xt > kt.time() + 3600 then
         return kt.Visitor.NOP
      end
      return value, 3600
   end
   if not db:accept(key, visit) then
      return kt.RVEINTERNAL
   end
   return kt.RVSUCCESS
end
