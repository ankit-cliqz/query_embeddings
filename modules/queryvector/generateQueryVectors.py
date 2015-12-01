'''
File Seek Version for Chunking + MultiProcessing to Generate Query Vectors
'''

import sys
import multiprocessing
from itertools import izip_longest


if len(sys.argv) != 5:
    print 'Usage: python generateQueryVectors.py <input_file> <output_file>  <num_processes> <num_file_chunks>''
    sys.exit()


inputfilepath = sys.argv[1]
outputfilepath = sys.argv[2]
num_processes = int(sys.argv[3])
num_chunks = int(sys.argv[4])

def file_block(fp, number_of_blocks, block):
    '''
    A generator that splits a file into blocks and iterates
    over the lines of one of the blocks.
    Source: https://xor0110.wordpress.com/2013/04/13/how-to-read-a-chunk-of-lines-from-a-file-in-python/

    '''

    assert 0 <= block and block < number_of_blocks
    assert 0 < number_of_blocks

    fp.seek(0,2)
    file_size = fp.tell()

    ini = file_size * block / number_of_blocks
    end = file_size * (1 + block) / number_of_blocks

    if ini <= 0:
        fp.seek(0)
    else:
        fp.seek(ini-1)
        fp.readline()

    while fp.tell() < end:
        yield fp.readline()

def grouper(n, iterable, padvalue=None):
	"""grouper(3, 'abcdefg', 'x') -->
	('a','b','c'), ('d','e','f'), ('g','x','x')"""
	return izip_longest(*[iter(iterable)]*n, fillvalue=padvalue)

def main():
    fw = open(outputfilepath, 'a')
    print "\nNumber of Processes: "+str(num_processes)
    print "\nNumber of Input File Chunks: "+str(num_chunks)

    fo = open(inputfilepath)
    for chunk_number in range(num_chunks):
        print "File Chunk Num: "+ str(chunk_number), 50 * '='
        inputdata_list= []
        for line in file_block(fo, num_chunks, chunk_number):
            if not line.strip() == "":
                inputdata_list.append(line.strip().decode('utf-8'))

        p = multiprocessing.Pool(num_processes)
        for chunk in grouper(1000, inputdata_list):
            try:
                results = p.map(preprocess_line, chunk)
                for r in results:
                    fw.write(r.encode("utf-8"))
            except:
                pass

        p.close()
    print "\nInput text file pre-processing is complete!"
    print "Finished!"
    fo.close()
    fw.close()



if __name__=="__main__":
    main()