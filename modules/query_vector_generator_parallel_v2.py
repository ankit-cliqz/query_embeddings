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

if len(sys.argv) != 6:
    print 'Usage: python query_vector_generator_parallel.py <input_file> <output_directory> <raw_vector_file> <vector_size> <number_of_cpus>'
    sys.exit()

inputfilepath = sys.argv[1]
output_location = sys.argv[2]
vectorsfilepath = sys.argv[3]
vector_size = int(sys.argv[4])
numJobs = int(sys.argv[5])

def getCorpusDict(vectorsfilepath):
    """
    Gets the vocab_dict, vocab, repvector from the word embeddings word vector.
    :rtype : Tuple with (vocab_dict: Dictionary, vocab: List, repvector: List)
    """
    vocab = []
    repvector = []
    # Size of Vector {Default: W2V=100, Poly=64, Glove=50}
    print "\nStarting Caching of the Word Embedding Vector File in Memory..."
    # Raw Vector File
    with open(vectorsfilepath) as p:
        for line in p:
            # if line.rstrip('\n') != str(sample_size)+' '+str(vector_size):
            vector_components = line.split(' ')
            vocab.append(vector_components[0])
            repvector.append(map(float, vector_components[1:vector_size + 1]))
    p.close()
    vocab_dict = {v: i for i, v in enumerate(vocab)}
    print "\nWord Embedding Vectors successfully cached!"
    return (vocab_dict, numpy.asarray(repvector))


def generateQueryAndQueryVectorMap(line_tmp, vocab_dict, repvector):
    sentencevector = []
    # print "Number of Records Left:\t" + str(corpuscount - tmpcount)
    query = line_tmp.lower()
    component_word = query.split(' ')
    for one_word in component_word:
        if one_word in vocab_dict:
            indexnum = vocab_dict.get(one_word)
            sentencevector.append((repvector[indexnum]).tolist())
        else:
            sentencevector.append([float(0)] * vector_size)
    l = numpy.array(sentencevector)
    # Centroid Calculation - each sentence.
    # Sums up all vectors (columns) and generates a final list (1D)of size vector_size
    lmt = numpy.array(l.sum(axis=0, dtype=numpy.float32)).tolist()
    # Averages the vectors based on the number of words in each sentence.
    query_vector = [x / len(component_word) for x in lmt]
    # query_queryvector_map[line] = query_vector
    return (query, query_vector)


def main():
    os.system("taskset -p 0xff %d" % os.getpid())
    corpus = []
    query_queryvector_map_file = os.path.join(output_location, "query_queryvector_map.pkl")
    repvector_file = os.path.join(output_location, "repvector_nparray.pkl")

    query_queryvector_map = {}

    print "\nStarting ... "
    print "\nNow caching the corpus list with the list of queries... "

    # Load the Query Data
    corpus = joblib.load(inputfilepath)

    print "\nCaching of corpus list complete!"
    corpuscount = len(corpus)

    vocab_dict, repvector = getCorpusDict(vectorsfilepath)

    # Dump the large numpy array to disk


    if not os.path.exists(repvector_file):
        print "Dump the large numpy array to disk"
        joblib.dump(repvector,repvector_file)
        print "Dumping of the Vector file to Disk complete!"

    # Load the repvector into the memory map -- Shared memory to be used by the processes.
    print "Loading the representation vector into the memory map."
    repvector_memmap = joblib.load(repvector_file, mmap_mode='r+')


    print "\nStarting Query Vector Computation ... "

    # Multi-Processing Code using job-lib
    # Initiating Parallel jobs for compute intensive task of generating sentence vectors.
    # max_nbytes=None,
    results = Parallel(n_jobs=numJobs,  max_nbytes=None, verbose=10)(delayed(generateQueryAndQueryVectorMap)(line_tmp, vocab_dict, repvector_memmap) \
                                       for line_tmp in corpus[:100])

    # results = Parallel(n_jobs=numJobs,  max_nbytes=None, verbose=10)(delayed(generateQueryAndQueryVectorMap)(line_tmp, vocab_dict, repvector) \
    #                                     for line_tmp in corpus)

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
