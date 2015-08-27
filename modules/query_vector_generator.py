# Query Vector Generator with different representation weights (Word2Vec, Polyglot and GloVe) for Training Data
# Version 1 : Tried to optimize space and running time of the code by reducing use of certain intermediate datastructures.
# @author: Ankit Bahuguna
import sys
import scipy.sparse
import numpy
import cPickle
import joblib


if len(sys.argv)!=5:
	print 'Usage: python query_vector_generator.py <input_file> <output_directory> <raw_vector_file> <vector_size>'
	sys.exit()

inputfilepath = sys.argv[1]
output_location = sys.argv[2]
vectorsfilepath= sys.argv[3]
vector_size = int(sys.argv[4])
corpus = []

query_queryvector_map_file = os.path.join(output_location, "query_queryvector_map.pkl")


# Load the Query Data
corpus = joblib.load(inputfilepath)

query_queryvector_map = {}

repvector = []
vocab=[]
tokens = []
sentencevectortemp = []
sentencevector = []
corpusvector=[]

#Size of Vector {Default: W2V=100, Poly=64, Glove=50}

# Raw Vector File
with open(vectorsfilepath) as p:
	for line in p:
		#if line.rstrip('\n') != str(sample_size)+' '+str(vector_size):
		component2 = line.split(' ')
		vocab.append(component2[0])
		repvector.append(map(float,component2[1:vector_size+1]))
p.close()
vocab_dict = {v:i for i,v in enumerate(vocab)}

for line_tmp in corpus:
	line = line_tmp.lower().encode("utf-8")
	component_word = line.split(' ')
	for one_word in component_word:
		if one_word in  vocab_dict:
			indexnum = vocab_dict.get(one_word)
			sentencevectortemp.append(repvector[indexnum])
		else
			sentencevectortemp.append([float(0)] * vector_size)
	
	l = numpy.array(sentencevectortemp)
	# Centroid Calculation - each sentence.
	# Sums up all vectors (columns) and generates a final list (1D)of size vector_size
	lmt = numpy.array(l.sum(axis=0, dtype=numpy.float32)).tolist()
	# Averages the vectors based on the number of words in each sentence.
	query_vector = [x / len(component_word) for x in lmt]
	query_queryvector_map[line] = query_vector

print 'Vector population dumped to disk ... '
joblib.dump(query_queryvector_map, query_queryvector_map_file)
print 'Data successfully dumped to disk!'