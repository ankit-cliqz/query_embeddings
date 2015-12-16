# Query Vector Generator with different representation weights (Word2Vec, Polyglot and GloVe) for Training Data
# Version 1 : Tried to optimize space and running time of the code by reducing use of certain intermediate datastructures.
# @author: Ankit Bahuguna
import sys
import scipy.sparse
import numpy
import cPickle
import joblib
import os
from joblib import Parallel, delayed

if len(sys.argv) != 5:
    print 'Usage: python query_vector_generator_parallel.py <input_file> <output_directory> <raw_vector_file> <vector_size>'
    sys.exit()

inputfilepath = sys.argv[1]
output_location = sys.argv[2]
vectorsfilepath = sys.argv[3]
vector_size = int(sys.argv[4])
corpus = []

query_queryvector_map_file = os.path.join(output_location, "query_queryvector_map.pkl")
print  "\nStarting ... "
print "\nNow caching the corpus list with the list of queries... "
# Load the Query Data
corpus = joblib.load(inputfilepath)
print "\nCaching of corpus list complete!"
query_queryvector_map = {}

repvector = []
vocab = []
tokens = []
sentencevectortemp = []
sentencevector = []
corpusvector = []
numJobs = 2

# Size of Vector {Default: W2V=100, Poly=64, Glove=50}
print "\nStarting Caching of the Word Embedding Vector File in Memory..."
# Raw Vector File
with open(vectorsfilepath) as p:
    for line in p:
        # if line.rstrip('\n') != str(sample_size)+' '+str(vector_size):
        component2 = line.split(' ')
        vocab.append(component2[0])
        repvector.append(map(float, component2[1:vector_size + 1]))
p.close()
vocab_dict = {v: i for i, v in enumerate(vocab)}
print "\nWord Embedding Vectors successfully cached!"
corpuscount = len(corpus)
# tmpcount = 1
print "\nStarting Query Vector Computation ... "


def generateQueryAndQueryVectorMap(line_tmp):
    # print "Number of Records Left:\t" + str(corpuscount - tmpcount)
    line = line_tmp.lower()
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
    query_vector = [x / len(component_word) for x in lmt]
    # query_queryvector_map[line] = query_vector
    return (line, query_vector)


# tmpcount = tmpcount + 1


# for line_tmp in corpus:
#	generateQueryAndQueryVectorMap()



# Parallization Code
# Initiating Parallel jobs for compute intensive task of generating sentence vectors.
results = Parallel(n_jobs=numJobs, verbose=10000)(delayed(generateQueryAndQueryVectorMap)(line_tmp) \
                                   for line_tmp in corpus)
# Aggregate the results into the query_queryvector_map dict.
for indiv_res in results:
    key, value = indiv_res
    query_queryvector_map[key] = value

print "\nQuery Vector Computation finished!"
print 'Vector population dumped to disk ... '
joblib.dump(query_queryvector_map, query_queryvector_map_file)
print 'Data successfully dumped to disk!'
