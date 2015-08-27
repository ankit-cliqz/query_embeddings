#!/usr/local/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ankit'

import msgpack

query_data_filename = "/Users/ankit/Documents/cliqz/query_embeddings/data/er_gc.mpack"
output_location = "/Users/ankit/Documents/cliqz/query_embeddings/output_data/"
outputfilepath = output_location+'1000_records.mpack'
outputfile = open(outputfilepath, 'w')

itemcount = 0
unpacker = msgpack.Unpacker(open(query_data_filename, 'r'))
#print "Starting ... "
for query_record in unpacker:
    itemcount += 1
    #print "Currently processing record: " + str(itemcount)
    if (itemcount < 1000):
        #outputfile.write(query_record)
        print query_record
    else:
        break

#print "1000 records written to disk"

