__author__ = 'ankit'
import numpy
from scipy import spatial
import sys



# Raw Vector File
def get_raw_word_embeddings(vectorsfilepath, vector_size):
    print "Starting to Cache Raw Word Embeddings..."
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


def computeQueryVector(query_string, vocab_dict, repvector, vector_size):
    line = query_string.lower()
    sentencevectortemp = []
    component_word = line.split(' ')
    for one_word in component_word:

        if one_word in vocab_dict:
            indexnum = vocab_dict.get(one_word)
            sentencevectortemp.append(repvector[indexnum])
        else:
            print "Warning: The word  '"+str(one_word)+"' in Query not in Word Embedding Matrix."
            sentencevectortemp.append([float(0)] * vector_size)
    l = numpy.array(sentencevectortemp)
    # Centroid Calculation - each sentence.
    # Sums up all vectors (columns) and generates a final list (1D)of size vector_size
    lmt = numpy.array(l.sum(axis=0, dtype=numpy.float32)).tolist()
    # Averages the vectors based on the number of words in each sentence.
    lmt[:] = [x / len(component_word) for x in lmt]
    output = numpy.asarray(lmt[:])
    return output


def computeCosineDistance(vector1, vector2):
    return 1 - spatial.distance.cosine(vector1, vector2)

def main():
    print "Initializing ... "
    if len(sys.argv) != 2:
        print 'Usage: python query_distance.py <word_embedding_vector_file>'
        print '<word_embedding_vector_file>: The pre-computed word embedding file'
        sys.exit()


    vectorsfilepath = sys.argv[1]
    vector_size = 100
    vocab_dict, repvector = get_raw_word_embeddings(vectorsfilepath,vector_size)


    print "\nQuery Vector Cosine Distance Calculator"
    print "-------------------------------------------"
    #print "[Cosine Distance Value close to 1 signifies more similar.]\n"

    flag = "True"
    while (flag == "True"):
        query1 = raw_input("Please enter first query: ")
        query2 = raw_input("Please enter Second query: ")

        query1vec = computeQueryVector(query1.rstrip(" ").lower(), vocab_dict, repvector, vector_size)
        query2vec = computeQueryVector(query2.rstrip(" ").lower(), vocab_dict, repvector, vector_size)

        cosdistance = computeCosineDistance(query1vec,query2vec)
        print "Cosine Distance: " +str(cosdistance)

        user_input = raw_input("\nDo you wish to continue again? (Type 'yes' to continue): ")
        if user_input == "yes":
            print "\n"
            continue
        else:
            print "Good Bye!"
            break

if __name__ == "__main__":
    main()