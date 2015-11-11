__author__ = 'ankit'
import pykeyvi
import os

keyviquerydatadir = ""
high_freq_queries = {}

for root, dirs, files in os.walk(keyviquerydatadir):
		path = root.split('/')
		for file in files:
			if (file.lower().startwith('query_ucrawl')):
                keyvifilePath = os.path.join(root,str(file))
                d = pykeyvi.Dictionary(keyvifilePath)
                allkeys = d.GetAllKeys()
                for query in allkeys:
                    query_parts = query.split(" ")
                    if len(query_parts) >=3:
                        #print query_parts[1:]
                        q = query_parts[1:]
                        if high_freq_queries.has_key(q):
                            high_freq_queries[q] = high_freq_queries.get(q) + 1
                        else:
                            high_freq_queries[q] = 1



