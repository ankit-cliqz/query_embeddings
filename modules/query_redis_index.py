# Adding Query Index to Redis - Multi-Process Version with a directory having File Splits

#!/usr/local/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ankit'
import sys
import multiprocessing
from multiprocessing.dummy import Pool as ThreadPool
from itertools import izip_longest
import os
import redis
from cache.db.query_norm import QueryNorm


if len(sys.argv) != 2:
    print 'Usage: python query_redis_index.py <num_processes>'
    sys.exit()


num_processes = int(sys.argv[1])


files_list = ["out_xaa",  "out_xab",  "out_xac",  "out_xad",  "out_xae",  "out_xaf",  "out_xag",  "out_xah",  "out_xai",  "out_xaj",  "out_xak",  "out_xal",  "out_xam",  "out_xan",  "out_xao",  "out_xap",  "out_xaq",  "out_xar",  "out_xas",  "out_xat",  "out_xau",  "out_xav",  "out_xaw",  "out_xax",  "out_xay",  "out_xaz",  "out_xba",  "out_xbb",  "out_xbc",  "out_xbd"]
#files_list = ["xaa",  "xab",  "xac",  "xad",  "xae",  "xaf",  "xag",  "xah",  "xai",  "xaj",  "xak",  "xal",  "xam",  "xan",  "xao",  "xap",  "xaq",  "xar",  "xas",  "xat"]

redis_handle = redis.StrictRedis(host='localhost', port=6384, db=0)


def grouper(n, iterable, padvalue=None):
	"""grouper(3, 'abcdefg', 'x') -->
	('a','b','c'), ('d','e','f'), ('g','x','x')"""
	return izip_longest(*[iter(iterable)]*n, fillvalue=padvalue)


def index_query_redis(line):
    components = line.split("\t\t")
    query = components[0]
    index_num= components[1]
    if isinstance(query, unicode):
        try:
            result = query.encode("utf-8")
        except:
            pass
    redis_handle.set(query,index_num)

print "Starting normalization of all files in directory ....."
print "\nNumber of Parallel Processes: "+str(num_processes)


qn_no_bow = QueryNorm.no_bow_normalizer()

index_num = 0
path = "/ebs/topqueries/queries_30out/"
for file in files_list:
    print "File: "+str(file)
    FilePath = os.path.join(path, str(file))
    with open(FilePath) as f1:
        inputdata_list= []
        for line in f1:
            if not line.strip():
                continue
            #parts = line.split("\t")
            #line = parts[0].strip()
            line = qn_no_bow.normalize(line)
            if not isinstance(line, unicode):
                try:
                    line = line.decode("utf-8")
                except:
                    pass

            record = "qvin:"+line.strip()+"\t\t"+str(index_num)
            inputdata_list.append(record)
            index_num +=1

    print index_num
    # Create Process pool (p)
    #p = multiprocessing.Pool(num_processes)
    p = ThreadPool(num_processes)
    for chunk in grouper(10000, inputdata_list):
        try:
            p.map(index_query_redis, chunk)
        except:
            pass
    # Close Multi-Processing pool
    p.join()
    p.close()
    print "\nFile: "+str(file)+" indexing is complete!"
print "Finished!"