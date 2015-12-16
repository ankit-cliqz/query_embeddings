from annoy import AnnoyIndex
import sys
import redis
from cache.db.query_norm import QueryNorm
from  queryembeddings.query_embedding_w2v import QueryEmbeddings
import time


qn_no_bow = QueryNorm.no_bow_normalizer()
qembed = QueryEmbeddings()
redis_handle = redis.Redis("localhost", 6384)


def getQueryVector(query):
    query_norm = qn_no_bow.normalize(query)
    qVec, wnf = qembed.generateQueryVector(query_norm, 100)
    #queryvec_str = " ".join(str(v) for v in qVec)
    #output = query_norm.strip() +"\t"+queryvec_str+"\n"
    return qVec


def get_single_querylist(ann_model,test_queryvector, nearest_num, redis_prefix):
    querylist = []
    query_index, distance = ann_model.get_nns_by_vector(test_queryvector, nearest_num, search_k=-1, include_distances=True)
    for index_num in query_index:
        querylist.append(redis_handle.get(redis_prefix + str(index_num)))
    return querylist, distance


def get_similar_queries(testquery, nearest_num, ann1, ann2, ann3, ann4, ann5):
    start = int(round(time.time() * 1000))
    # Compute query Vector
    test_queryvector = getQueryVector(testquery.strip())

    querylist1, dist1 = get_single_querylist(ann1,test_queryvector, nearest_num, "qvin1:")
    querylist2, dist2 = get_single_querylist(ann2,test_queryvector, nearest_num, "qvin2:")
    querylist3, dist3 = get_single_querylist(ann3,test_queryvector, nearest_num, "qvin3:")
    querylist4, dist4 = get_single_querylist(ann4,test_queryvector, nearest_num, "qvin4:")
    querylist5, dist5 = get_single_querylist(ann5,test_queryvector, nearest_num, "qvin5:")

    # Add all the items to one long list of items
    queries_lsh = querylist1 + querylist2 + querylist3 + querylist4 + querylist5
    dist = dist1 + dist2 + dist3 + dist4 + dist5
    # Zip them together
    lsh_list_n = zip(queries_lsh, dist)
    # Sort the List on the basis of Distances, with the shortest distance first.
    lsh_list_n.sort(key=lambda tup: tup[1])
    end = int(round(time.time() * 1000))
    print "Total Time: "+ str(end-start) + " milli-seconds."
    return lsh_list_n

def main():

    # Annoy Vector Dimension
    vec_dimension =100

    models_dir = "/raid/ankit/ann_models/"
    start = time.time()
    print "Starting: Loading of memory mapped models ... "
    # Load all models - memory mapped - quick
    ann1 = AnnoyIndex(vec_dimension)
    ann1.load(models_dir+"model10_split1.ann")

    ann2 = AnnoyIndex(vec_dimension)
    ann2.load(models_dir+"model10_split2.ann")

    ann3 = AnnoyIndex(vec_dimension)
    ann3.load(models_dir+"model10_split3.ann")

    ann4 = AnnoyIndex(vec_dimension)
    ann4.load(models_dir+"model10_split4.ann")

    ann5 = AnnoyIndex(vec_dimension)
    ann5.load(models_dir+"model10_split5.ann")
    end =time.time()

    print "All annoy-lsh models loaded! Time Taken: "+str((end-start)/60)+ " minutes."


    print "\nSimilar Queries - LSH Interface [All Top Queries]"
    print "----------------------------------------------------"

    flag = "True"
    while (flag == "True"):
        testquery = raw_input("Enter Query: ")
        nearest_num = raw_input("Number of similar queries: ")
        if nearest_num == 0 or nearest_num == "":
            nearest_num = 10
        nearest_num = int(nearest_num)
        if not testquery.strip() =="":
            lsh_list_n = get_similar_queries(testquery.strip(), nearest_num, ann1, ann2, ann3, ann4, ann5)

            # Return and Print the Top 10 nearest Queries to the Original Query
            print "\nCandidate Nearest Queries [TOP 10]: "
            count = 0
            for query,distance in lsh_list_n:
                if count == nearest_num:
                    break
                print str(query)+"\t"+str(distance)
                count+=1

            user_input = raw_input("\nDo you wish to continue again? (Type 'no' to quit): ")
            if user_input == "no":
                print "\nGoodbye!"
                break
            else:
                print "\n"
                continue


if __name__=="__main__":
    main()

