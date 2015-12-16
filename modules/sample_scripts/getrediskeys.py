import redis

r = redis.Redis("localhost", 6384)
fw = open("rediskeys_python", "w")
count = 1
for key in r.scan_iter():
    if count % 100000 == 0:
        print "Done: " + str(count)
    key = key.replace("qvin:")
    count += 1
    fw.write(str(key) + "\n")
fw.close()
