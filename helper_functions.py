from scipy import spatial
import numpy

# Cosine Distance: 1 => Similar; 0 => Dis-similar 
def computeCosineDistance(vector1, vector2):
	return 1 - spatial.distance.cosine(vector1, vector2)


# Returns a Boolean: True if all items of the vetors are equal, false other wise
def checkVectorEquality(array1, array2):
	if (array1 == array2).all():
		return true
	else:
		return false

# Returns a numpy array which represents the query in word embedding vector form.
# Internally computes the centroid for the query vector. Idea is that the query vector is represented by 
# the centroid of individual word vectors. 
# Requires global: vector_size, vocab_dict
def computeQueryVector(query_string):
	line = query_string.lower().encode("utf-8")
	sentencevectortemp = []
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
	lmt[:] = [x / len(component_word) for x in lmt]
	output = numpy.asarray(lmt[:])
	return output



# Step 1: Top 10 similar queries

def topTenSimilarQueries(query_string):
	unk_query_vector = computeQueryVector(query_string)
	for key, value in query_queryvector_map.iteritems():
		





# Step 2: Associated pages with the query []

# Step 3: Check: If results returned match one of the expected results([]) 