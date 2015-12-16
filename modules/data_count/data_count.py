#!/usr/local/bin/python
# -*- coding: utf-8 -*-


# This script is used to process the er_gc.mpack query log data
# The script stores all the requisite queries and query-page model on the disk.
# @uthor: Ankit Bahuguna
# @email: ankit@cliqz.com

import msgpack
import sys
import json
import joblib
import os

'''
# Command Line Arguments - Uncomment this, if you want to use this as an independent customized command line script. 

if len(sys.argv)!=3:
    print '\nUsage: data_count.py <querydata_input_file> <output_directory>'
    print '\nquerydata_input_file: Query data input file, in .mpack format' 
    print '\noutput_directory: Output directory to store all processed data as .pkl files.' 
    sys.exit()
'''

query_data_filename = "/Users/ankit/Documents/cliqz/query_embeddings/data/er_gc.mpack"
output_location = "/Users/ankit/Documents/cliqz/query_embeddings/output_data/"

original_query_list_file = os.path.join(output_location, "original_query_list.pkl")
expected_result_list_file = os.path.join(output_location, "expected_result_list.pkl")
originalquery_exresult_file = os.path.join(output_location, "originalquery_exresult.pkl")
queries_returned_list_file = os.path.join(output_location, "queries_returned_list.pkl")
original_query_puc_list_file = os.path.join(output_location, "original_query_puc_list.pkl")
url_query_map_file = os.path.join(output_location, "url_query_map.pkl")

original_query_list = []  # Simple List of Query Strings
expected_result_list = []  # List of List of Expected Results
originalquery_exresult = {}  # Mapping of the queries with the expected result list (urls) <String , List[]>
queries_returned_list = []  # List of all the queries returned by the Big Machine
original_query_puc_list = {}  # Mapping of original queries with the expected list of urls
url_query_map = {}  # Mapping of the PUC URL (Page) as key and the list of queries which leads to that page.

itemcount = 0
unpacker = msgpack.Unpacker(open(query_data_filename, 'r'))

for query_record in unpacker:
    itemcount += 1
    print "Currently processing record: " + str(itemcount)
    record_json_dump = json.dumps(query_record)
    single_record = json.loads(record_json_dump)

    # Original Query 
    query_original = single_record["query"].encode("utf-8")
    # print "[original_query]\t"+str(query_original)
    original_query_list.append(query_original)

    # Expected result returned by the system. 
    expected_result = single_record["ex"]
    ex_res_count = 0
    indiv_expected_result = []
    for ex in expected_result:
        # print "[expected_result]("+str(ex_res_count)+")\t"+str(ex.encode("utf-8"))
        indiv_expected_result.append(ex.encode("utf-8"))
        ex_res_count = ex_res_count + 1
    expected_result_list.append(indiv_expected_result)

    # Maps the original query to all its expected results
    originalquery_exresult[str(query_original)] = indiv_expected_result

    puc_url_list = []
    # Iterate over the keys to get all the keys (URL) associated with the given query.
    for key in single_record["ex_data"].keys():
        url = str(key.encode("utf-8"))
        # print "URL: "+ url
        puc_url_list.append(url)
        indiv_query = []
        if not (single_record["ex_data"].get(key) is None):
            list3 = single_record["ex_data"].get(key)
            if not list3 is None:
                ucrawl_queries = list3["info"]["tq"]["ucrawl"]
                qs_queries = list3["info"]["tq"]["qs"]
                qc_queries = list3["info"]["tq"]["qc"]
                if not ucrawl_queries is None:
                    for q_ucrawl in ucrawl_queries:
                        for tr_ucrawl in q_ucrawl:
                            if (isinstance(tr_ucrawl, int)):
                                continue
                            else:
                                # print str("[ucrawl]\t")+tr.encode("utf-8")
                                queries_returned_list.append(tr_ucrawl.encode("utf-8"))
                                indiv_query.append(tr_ucrawl.encode("utf-8"))
                if not qc_queries is None:
                    for q_qc in qc_queries:
                        for tr_qc in q_qc:
                            if (isinstance(tr_qc, int)):
                                continue
                            else:
                                # print str("[qc]\t")+tr.encode("utf-8")
                                queries_returned_list.append(tr_qc.encode("utf-8"))
                                indiv_query.append(tr_qc.encode("utf-8"))
                if not qs_queries is None:
                    for q_qs in qs_queries:
                        for tr_qs in q_qs:
                            if (isinstance(tr_qs, int)):
                                continue
                            else:
                                # print str("[qs]\t")+tr.encode("utf-8")
                                queries_returned_list.append(tr_qs.encode("utf-8"))
                                indiv_query.append(tr_qs.encode("utf-8"))
        url_query_map[url] = indiv_query
    original_query_puc_list[query_original] = puc_url_list
print "All records processed. Currently writing to disk!"

# JobLib Dump to Disk
joblib.dump(original_query_list, original_query_list_file)
joblib.dump(expected_result_list, expected_result_list_file)
joblib.dump(originalquery_exresult, originalquery_exresult_file)
joblib.dump(queries_returned_list, queries_returned_list_file)
joblib.dump(original_query_puc_list, original_query_puc_list_file)
joblib.dump(url_query_map, url_query_map_file)

print "All data written to disk!"
