require 'memcache'

host = "127.0.0.1:11211"
printf("connecting...\n")
cache = MemCache.new([host], {})
printf("count: %s\n", cache.stats[host]["curr_items"])

printf("flusing...\n")
cache.flush_all
printf("count: %s\n", cache.stats[host]["curr_items"])

printf("setting...\n")
(1..10).each do |i|
  key = sprintf("%08d", i)
  value = sprintf("%d", i)
  cache.set(key, value, 180, { :raw => true })
  cache.add(key, value, 180, { :raw => true })
  cache.replace(key, value, 180, { :raw => true })
end
printf("count: %s\n", cache.stats[host]["curr_items"])

printf("getting...\n")
(1..10).each do |i|
  key = sprintf("%08d", i)
  cache.get(key, { :raw => true })
end
printf("count: %s\n", cache.stats[host]["curr_items"])

printf("incrementing...\n")
(1..10).each do |i|
  key = sprintf("%08d", i)
  cache.incr(key, 1)
  cache.decr(key, 1)
end
printf("count: %s\n", cache.stats[host]["curr_items"])

printf("removing...\n")
(1..10).each do |i|
  key = sprintf("%08d", i)
  cache.delete(key)
end
printf("count: %s\n", cache.stats[host]["curr_items"])
