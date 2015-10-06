from scipy import spatial
import numpy

# Cosine Distance: 1 => Similar; 0 => Dis-similar 
def computeCosineDistance(vector1, vector2):
	return 1 - spatial.distance.cosine(vector1, vector2)


# Returns a Boolean: True if all items of the vetors are equal, false other wise
def checkVectorEquality(array1, array2):
	if (array1 == array2).all():
		return True
	else:
		return False


# Removes (,), ", ', ., "  ", "   " ,  „  and “

def preprocess_line(line):
    line_tmp1 = line.replace("(", " ")
    line_tmp2 = line_tmp1.replace(")", " ")
    line_tmp3 = line_tmp2.replace("\"", " ")
    line_tmp4 = line_tmp3.replace("\'", " ")
    line_tmp5 = line_tmp4.replace('„', " ")
    line_tmp6 = line_tmp5.replace('“', " ")
    line_tmp7 = line_tmp6.replace('.', " . ")
    line_tmp8 = line_tmp7.replace('  ', " ")
    line_tmp9 = line_tmp8.replace('   ', " ")
    return line_tmp9