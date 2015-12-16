__author__ = 'ankit'
import joblib

path = "/ebs/output_data_new/originalquery_queriesreturnedlist_map.pkl"

d = joblib.load(path)

print "Start"
# Frequency based map
wordfrequencymap = {}

query_output_file = open("/ebsnew/allqueriesoutput.txt", 'w')
dictfile = "/ebsnew/dictfile.pkl"
def addToWordFreqMap(words):
    for w in words:
        if w in (wordfrequencymap):
            wordfrequencymap[w]+= 1
        else:
            wordfrequencymap[w]= 1



for original_query, query_list in d.iteritems():
    #print original_query+"\n"
    words = original_query.split(" ")
    if len(words) >= 2:
        query_output_file.write(original_query.strip()+"\n")
        addToWordFreqMap(words)
    for query in query_list:


        #print q+"\n"
        qwords = query.split(" ")
        if len(qwords) >= 2:
            query_output_file.write(query.strip()+"\n")
            addToWordFreqMap(words)

print  "Done!"
query_output_file.close()
#joblib.dump(wordfrequencymap, dictfile)