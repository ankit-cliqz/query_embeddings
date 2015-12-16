import time
files = ['out_xaa', 'out_xab', 'out_xac', 'out_xad', 'out_xae', 'out_xaf', 'out_xag', 'out_xah', 'out_xai', 'out_xaj', 'out_xak', 'out_xal', 'out_xam', 'out_xan', 'out_xao', 'out_xap', 'out_xaq', 'out_xar', 'out_xas', 'out_xat', 'out_xau', 'out_xav', 'out_xaw', 'out_xax', 'out_xay', 'out_xaz', 'out_xba', 'out_xbb', 'out_xbc', 'out_xbd']
#files = ['out_xai', 'out_xaj', 'out_xak', 'out_xal', 'out_xam', 'out_xan', 'out_xao', 'out_xap', 'out_xaq', 'out_xar', 'out_xas', 'out_xat', 'out_xau', 'out_xav', 'out_xaw', 'out_xax', 'out_xay', 'out_xaz', 'out_xba', 'out_xbb', 'out_xbc', 'out_xbd']
query_vectors_directory = '/raid/ankit/30redisout/'


def normalize_redis_vector(vector_tmp):
    vector_tmp = vector_tmp.split()
    vector_norm = map(float, vector_tmp)
    return vector_norm


def create_index(file_list):

    for f in file_list:
        i = 0
        print "Processing {} ...".format(f)
        with open(query_vectors_directory+f) as cur_f:
            for line in cur_f:
                if not line.strip():
                    continue
                queryset = set()
                if i%1000000 == 0:
                    print "{} lines complete.".format(i)
                try:

                    query, vector = line.split('\t')

                    if query in queryset:
                        print "Query Repeated:" + str(query)
                    else:
                        queryset.add(query)
                    #vector = normalize_redis_vector(vector)
                except:
                    print "LineNum: "+str(i)+"\t"+line
                    pass
                i+=1

        print "File consistency check is complete for file {}".format(f)
    #return i


if __name__=="__main__":


    print "Starting Split 1 ...  "
    start = time.time()
    startcount1 = create_index(files[0:6])
    end  = time.time()
    print "Split 1  - Finished! Time Taken (Minutes) :" + str((end - start)/60)

    print "Starting Split 2 ...  "
    start = time.time()
    startcount2 = create_index(files[6:12])
    end =time.time()
    print "Split 2  - Finished! Time Taken (Minutes) :" + str((end - start)/60)

    print "Starting Split 3 ...  "
    start = time.time()
    startcount3 = create_index(files[12:18])
    end = time.time()
    print "Split 3  - Finished! Time Taken (Minutes) :" + str((end - start)/60)

    print "Starting Split 4 ...  "
    start =time.time()
    startcount4 = create_index(files[18:24])
    end = time.time()
    print "Split 4  - Finished! Time Taken (Minutes) :" + str((end - start)/60)

    print "Starting Split 5 ...  "
    start = time.time()
    startcount5 = create_index(files[24:])
    end = time.time()
    print "Split 5  - Finished! Time Taken (Minutes) :" + str((end - start)/60)

    print "Finished All Operations! "