# Query Vector Generator with different representation weights (Word2Vec, Polyglot and GloVe) for Training Data
# Version 1 : Tried to optimize space and running time of the code by reducing use of certain intermediate datastructures.
# Version 2 : Utilizes Joblib multiprocessing
# Version 3 : Utilizes the Redis backend for making the dictionary lookups faster and not causing memory dis-allocation problem.
# @author: Ankit Bahuguna
import sys
import scipy.sparse
import numpy
import cPickle
import joblib
import os
from joblib import Parallel, delayed
import redis


if len(sys.argv) != 6:
    print 'Usage: python query_vector_generator_parallel.py <input_file> <output_directory> <raw_vector_file> <vector_size> <number_of_cpus>'
    sys.exit()

inputfilepath = sys.argv[1]
output_location = sys.argv[2]
vectorsfilepath = sys.argv[3]
vector_size = int(sys.argv[4])
numJobs = int(sys.argv[5])

# Initialize a handle for the local redis server running on the system.
redis_handle = redis.StrictRedis(host='localhost', port=6379, db=0)

def getCorpusDict(vectorsfilepath):
    """
    Gets the vocab_dict, vocab, repvector from the word embeddings word vector.
    :rtype : Tuple with (vocab_dict: Dictionary, vocab: List, repvector: List)
    """
    print "\nStarting Caching of the Word Embedding Vector File in Redis (in memory database)..."
    # Raw Vector File
    with open(vectorsfilepath) as p:
        for line in p:
            # if line.rstrip('\n') != str(sample_size)+' '+str(vector_size):
            vector_components = line.split(' ')
            word =  vector_components[0]
            word_vector = map(float, vector_components[1:vector_size + 1])
            redis_handle.set(word, word_vector)
    p.close()
    print "\nWord Embedding Vectors successfully cached in Redis!"


def normalize_redis_vector(vector_tmp):
    vector_tmp1 = vector_tmp.replace("[", "")
    vector_tmp2 = vector_tmp1.replace("]", "")
    vector_tmp3 = vector_tmp2.split(",")
    vector_tmp4 = map(float, vector_tmp3)
    return vector_tmp4


def generateQueryAndQueryVectorMap(line_tmp):
    sentencevector = []
    # print "Number of Records Left:\t" + str(corpuscount - tmpcount)
    query = line_tmp.lower()
    component_word = query.split(' ')
    for one_word in component_word:
        if redis_handle.exists(one_word):
            vector_tmp = redis_handle.get(one_word)
            vector_final = normalize_redis_vector(vector_tmp)
            sentencevector.append(vector_final)
            #indexnum = vocab_dict.get(one_word)
            #sentencevector.append((repvector[indexnum]).tolist())
        else:
            sentencevector.append([float(0)] * vector_size)
    l = numpy.array(sentencevector)
    # Centroid Calculation - each sentence.
    # Sums up all vectors (columns) and generates a final list (1D)of size vector_size
    lmt = numpy.array(l.sum(axis=0, dtype=numpy.float32)).tolist()

    if (lmt != 0.0):
        # Averages the vectors based on the number of words in each sentence.
        query_vector = [x / len(component_word) for x in lmt]
    else:
        query_vector = [float(0)] * vector_size

    return (query, query_vector)


def main():
    os.system("taskset -p 0xff %d" % os.getpid())
    corpus = []
    query_queryvector_map_file = os.path.join(output_location, "query_queryvector_map.pkl")

    query_queryvector_map = {}

    print "\nStarting ... "
    print "\nNow caching the corpus list with the list of queries... "

    # Load the Query Data
    corpus = joblib.load(inputfilepath)
    print "\nCaching of corpus list complete!"

    #corpuscount = len(corpus)



    # Cache the word vectors to redis. - Uncomment this! commented just for testing!
    getCorpusDict(vectorsfilepath)
    print "\nStarting Query Vector Computation ... "

    # Multi-Processing Code using job-lib
    # Initiating Parallel jobs for compute intensive task of generating sentence vectors.
    # max_nbytes=None,
    results = Parallel(n_jobs=numJobs,  max_nbytes=None, verbose=25000)(delayed(generateQueryAndQueryVectorMap)(line_tmp) \
                                       for line_tmp in corpus)

    # Aggregate the results into the query_queryvector_map dict.
    for indiv_res in results:
        key, value = indiv_res
        query_queryvector_map[key] = value

    print "\nQuery Vector Computation finished!"
    print 'Vector population dumped to disk ... '
    joblib.dump(query_queryvector_map, query_queryvector_map_file)
    print 'Data successfully dumped to disk!'


if __name__ == "__main__":
    main()