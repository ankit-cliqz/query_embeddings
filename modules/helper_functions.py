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


