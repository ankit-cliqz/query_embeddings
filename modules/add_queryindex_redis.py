import redis
import sys

redis_handle = redis.Redis("localhost", 6384)

if len(sys.argv) != 3:
    print 'Usage: python add_queryindex_redis.py <redis_indexfile> <redis_prefix>'
    sys.exit()

redis_indexfile = sys.argv[1]
redis_prefix = sys.argv[2]
ri_directory = "/raid/ankit/"


def add_query_index_to_redis(redis_indexfile):
    with open(ri_directory+redis_indexfile, "r") as f:
        count = 0
        for line in f:
            line = line.strip()
            if not line=="":
                if count % 1000000 == 0:
                    print "Completed Indexing of "+ str(count)+ " records."
                try:
                    component = line.replace("\t\t","##$$##").split("##$$##")
                    redis_handle.set(redis_prefix+str(component[1]), str(component[0]))
                except Exception as e:
                    print "Error: "+str(e)+"\tLine: "+line
                    pass
                count+=1


if __name__=="__main__":
    print "Starting ... "+redis_indexfile
    #print "Redis Prefix:" + redis_prefix
    add_query_index_to_redis(redis_indexfile)
    print "Finished Indexing: "+redis_indexfile + " to Redis."