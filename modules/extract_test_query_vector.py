__author__ = 'ankit'
import joblib
import os
print "Start ... "
query_queryvector_map = {}
output_location = "/mnt/"
query_queryvector_txtfile = "/mnt/file0.final"
query_queryvector_map_file = os.path.join(output_location, "query_queryvector_map1M.pkl")

count =1
with open(query_queryvector_txtfile) as f:
    for line in f:
        if (count == 1000000):
            break
        component = line.split('\t')
        query = component[0]
        queryvector_tmp = component[1].rstrip()
        qv = queryvector_tmp.split(" ")
        queryvector = map(float, qv)
        query_queryvector_map[query] = queryvector
        count+=1

print "Now Dumping the Query Query Vector to Disk.."
joblib.dump(query_queryvector_map, query_queryvector_map_file)
print "Finished Dumping File to Disk!"


