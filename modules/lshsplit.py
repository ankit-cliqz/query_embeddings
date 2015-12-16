from annoy import AnnoyIndex
import numpy as np
import sys
#import redis
import time
#from queryembeddings.query_embedding_w2v import QueryEmbeddings


'''
if len(sys.argv) != 2:
    print 'Usage: python lshsplit.py <split_number>'
    print "\nSplit Number Help"  \
        "\n1: output file 'xaa' to 'xaf'" \
        "\n2: output file 'xag' to 'xal'" \
        "\n3: output file 'xam' to 'xar'" \
        "\n4: output file 'xas' to 'xax'" \
        "\n5: output file 'xay' to 'xbd'"
    sys.exit()

split_number = int(sys.argv[1])
'''


#redis_prefix = "qvin:"
#redis_handle = redis.StrictRedis(host='localhost', port=6384, db=0)
files = ['out_xaa', 'out_xab', 'out_xac', 'out_xad', 'out_xae', 'out_xaf', 'out_xag', 'out_xah', 'out_xai', 'out_xaj', 'out_xak', 'out_xal', 'out_xam', 'out_xan', 'out_xao', 'out_xap', 'out_xaq', 'out_xar', 'out_xas', 'out_xat', 'out_xau', 'out_xav', 'out_xaw', 'out_xax', 'out_xay', 'out_xaz', 'out_xba', 'out_xbb', 'out_xbc', 'out_xbd']

query_vectors_directory = '/raid/ankit/30redisout/'

def normalize_redis_vector(vector_tmp):
    vector_tmp = vector_tmp.split()
    vector_norm = map(float, vector_tmp)
    return vector_norm

def create_index(file_list, start_count,model_filename, redis_index_file):
    f = 100
    t = AnnoyIndex(f)
    t.verbose(True)
    redisindex = open("/raid/ankit/"+redis_index_file,"w")
    i = start_count
    for f in file_list:
        print "Processing {} ...".format(f)
        with open(query_vectors_directory+f) as cur_f:
            for line in cur_f:
                #print line

                if not line.strip():
                    continue

                if i%1000000 == 0:
                    print "{} lines complete.".format(i)
                query, vector = line.split('\t')
                vector = normalize_redis_vector(vector)
                redisindex.write(str(query)+"\t\t"+str(i)+"\n")
                try:
                    t.add_item(i,vector)
                except:
                    print "Exception : "+ str(line)
                    pass
                #print i
                i+=1

    print "Done adding items, now starting to build 10 trees.."
    t.build(10)
    print "Saving Model on Disk..."
    t.save('/raid/ankit/ann_models/'+model_filename)

    print "Finished Building and Saving Model!"
    redisindex.close()

    return i


if __name__=="__main__":
    # 80906640

    print "Starting Split 1 ...  "
    start = time.time()
    startcount1 = create_index(files[0:6], 0, "model10_split1.ann", "ri1.txt")
    end  = time.time()
    print "Total Queries Indexed: "+str(startcount1)
    print "Split 1  - Finished! Time Taken (Minutes) :" + str((end - start)/60)

    print "Starting Split 2 ...  "
    start = time.time()
    startcount2 = create_index(files[6:12],0,"model10_split2.ann", "ri2.txt")
    end =time.time()
    print "Total Queries Indexed: "+str(startcount2)
    print "Split 2  - Finished! Time Taken (Minutes) :" + str((end - start)/60)

    print "Starting Split 3 ...  "
    start = time.time()
    startcount3 = create_index(files[12:18],0,"model10_split3.ann", "ri3.txt")
    end = time.time()
    print "Total Queries Indexed: "+str(startcount3)
    print "Split 3  - Finished! Time Taken (Minutes) :" + str((end - start)/60)

    print "Starting Split 4 ...  "
    start =time.time()
    startcount4 = create_index(files[18:24],0,"model10_split4.ann","ri4.txt")
    end = time.time()
    print "Total Queries Indexed: "+str(startcount4)
    print "Split 4  - Finished! Time Taken (Minutes) :" + str((end - start)/60)

    print "Starting Split 5 ...  "
    start = time.time()
    startcount5 = create_index(files[24:],0,"model10_split5.ann","ri5.txt")
    end = time.time()
    print "Total Queries Indexed: "+str(startcount5)
    print "Split 5  - Finished! Time Taken (Minutes) :" + str((end - start)/60)

    print "Finished All Operations! "