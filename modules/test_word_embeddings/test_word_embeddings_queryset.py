from scipy import spatial
import numpy
import heapq
import sys
import scipy.sparse
import cPickle
from scipy import spatial
import os
import joblib
import logging

if len(sys.argv) != 2:
    print 'Usage: python test_word_embeddings.py <input_vector_file> '
    print 'input_vector_file: The pre-computed word embedding file'
    sys.exit()

output_location = "/ebs/output_data/"
output_location_new = "/ebs/output_data_new/"

#query_queryvector_parentmap = {}

## Load All the Data / Query / Page Models
original_query_list_file = os.path.join(output_location, "original_query_list.pkl")
expected_result_list_file = os.path.join(output_location, "expected_result_list.pkl")
originalquery_exresult_file = os.path.join(output_location, "originalquery_exresult.pkl")
queries_returned_list_file = os.path.join(output_location, "queries_returned_list.pkl")
original_query_puc_list_file = os.path.join(output_location, "original_query_puc_list.pkl")
url_query_map_file = os.path.join(output_location, "url_query_map.pkl")
#query_queryvector_map_file = os.path.join(output_location, "query_queryvector_map10000.pkl")

originalquery_queriesreturnedlist_map_file = os.path.join(output_location_new, "originalquery_queriesreturnedlist_map.pkl")

# TODO: Adding "query_queryvector_map". Right bow the
print "Starting to cache files in Memory ... "
original_query_list = joblib.load(original_query_list_file)
print "Original Query List Cached!"
expected_result_list = joblib.load(expected_result_list_file)
print "Expected Result List Cached!"
originalquery_exresult = joblib.load(originalquery_exresult_file)
print "Original Query - Expected result map Cached!"
#queries_returned_list = joblib.load(queries_returned_list_file)
#print "Queries Returned List Cached!"
original_query_puc_list = joblib.load(original_query_puc_list_file)
print "Original Query - PUC List map Cached!"
url_query_map = joblib.load(url_query_map_file)
print "URL: Query Map cached!"
#query_queryvector_map = joblib.load(query_queryvector_map_file)
#print "Query - Query Vector Map cached!"

originalquery_queriesreturnedlist_map = joblib.load(originalquery_queriesreturnedlist_map_file)
print "Originalquery - Queriesreturnedlist Map Cached"
print "File Caching in Memory Done!"
# Vector File Path
vectorsfilepath = sys.argv[1]

# Load Global Variables
vector_size = int(100)

# Number of similar queries in this given context.
top_n_similar_queries = int (10)

# Raw Vector File
def get_raw_word_embeddings(vectorsfilepath):
    print "Starting to Cache Raw Word Embeddings"
    vocab = []
    repvector = []
    with open(vectorsfilepath) as p:
        for line in p:
            # if line.rstrip('\n') != str(sample_size)+' '+str(vector_size):
            component2 = line.split(' ')
            vocab.append(component2[0])
            repvector.append(map(float, component2[1:vector_size + 1]))
    p.close()
    vocab_dict = {v: i for i, v in enumerate(vocab)}
    print "Raw Word Embeddings Cached!"
    return vocab_dict, repvector


vocab_dict, repvector = get_raw_word_embeddings(vectorsfilepath)


# Returns a numpy array which represents the query in word embedding vector form.
# Internally computes the centroid for the query vector. Idea is that the query vector is represented by 
# the centroid of individual word vectors. 
# Requires global: vector_size, vocab_dict
def computeQueryVector(query_string):
    line = query_string.lower()
    sentencevectortemp = []
    component_word = line.split(' ')
    for one_word in component_word:
        if one_word in vocab_dict:
            indexnum = vocab_dict.get(one_word)
            sentencevectortemp.append(repvector[indexnum])
        else:
            sentencevectortemp.append([float(0)] * vector_size)
    l = numpy.array(sentencevectortemp)
    # Centroid Calculation - each sentence.
    # Sums up all vectors (columns) and generates a final list (1D)of size vector_size
    lmt = numpy.array(l.sum(axis=0, dtype=numpy.float32)).tolist()
    # Averages the vectors based on the number of words in each sentence.
    lmt[:] = [x / len(component_word) for x in lmt]
    output = numpy.asarray(lmt[:])
    return output


# Cosine Distance: 1 => Similar; 0 => Dis-similar
def computeCosineDistance(vector1, vector2):
    return 1 - spatial.distance.cosine(vector1, vector2)

def get_query_queryvector_map(query_string):
    #print "Starting to compute Query And Query Vector Map"
    associated_queries = originalquery_queriesreturnedlist_map.get(query_string)
    query_queryvector_map_tmp = {}
    for single_query in associated_queries:
        qvector = computeQueryVector(single_query)
        query_queryvector_map_tmp[single_query] = qvector
        # Adding the same Query Query Vector Pair to a parent map for fast loading
        #query_queryvector_parentmap[single_query] = qvector
    #print "Computed - Query And Query Vector Map!"
    return query_queryvector_map_tmp

# Step 1: Get the top 10 similar queries -- Most compute Instensive.. Investigate a way to make this faster.
def getTopTenSimilarQueries(query_string):
    heap = []
    unk_query_vector = computeQueryVector(query_string)
    query_queryvector_map = get_query_queryvector_map(query_string)
    for query, query_vector in query_queryvector_map.iteritems():
        cosine_distance = computeCosineDistance(unk_query_vector, query_vector)
        heapq.heappush(heap, (-cosine_distance, cosine_distance, query))
    similar_query_list = []

    for x in xrange(0, top_n_similar_queries):
        if (len(heap) == 0):
            break
        _, distance, query_str = heapq.heappop(heap)
        similar_query_list.append(query_str)
    return similar_query_list


# Step 2: Associated pages with the query []
def getAssociatedPagesWithQuery(query_string):
    associated_url_list = []
    similar_query_list = getTopTenSimilarQueries(query_string)
    #for x in similar_query_list:
    #    print x
    for url, query_list in url_query_map.iteritems():
        # Convert the querylist into a set for faster lookups.
        query_list_set = set(query_list)
        for indiv_query in similar_query_list:
            if indiv_query in query_list_set:
                associated_url_list.append(url)
    return associated_url_list


# Step 3: Check: If results returned match one of the expected results([]) 
# We can normaize the url links here, by removing irrelevant info which can hamper results.
# Like two links which lead to the same page but have different results.
# Returns a Boolean True if returned url result match expected result
def checkReturnedListWithExpectedResults(query_string):
    associated_urls = getAssociatedPagesWithQuery(query_string)
    expected_result_list_query = originalquery_exresult.get(query_string)
    #print "Associated URL Size:" +str(len(associated_urls))
    for url in associated_urls:
        if url in expected_result_list_query:
            return True;
    return False


def main():
    '''
    Runs the Test for all the Original Queries in the logs and evaluates if
    the returned results were same as the expected results.
    '''
    print "Starting Test ..."
    count = 0
    correct_match = 0
    for original_query in original_query_list[:100]:
        #print "Original Query: "+str(original_query)
        count = count + 1
        boolvalue = checkReturnedListWithExpectedResults(original_query)
        if (boolvalue == True):
            correct_match = correct_match + 1
        print "Completed "+str(count) + " out of 100. Accuracy %: "+ str(float(correct_match) * 100 / float(count)  )
    print  "\n\nFinal Scores"
    print "=============="
    print "Total Original Queries:" + str(count)
    print "Total Result Returned:" + str(correct_match)
    print "Percentage - Correct results returned:" + str(float(correct_match) * 100 / float(count)) + "%"


if __name__== "__main__":
    main()
