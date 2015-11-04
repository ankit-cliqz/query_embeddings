__author__ = 'ankit'
import joblib
from operator import itemgetter
dictfile = "/ebsnew/dictfile.pkl"

d = joblib.load(dictfile)
f = open("/ebsnew/wordfrequencysorted.txt", 'w')
wordfreq = []

for key, value in d.iteritems():
    wordfreq.append((key,value))

wordfreq.sort(key=itemgetter(0))
wordfreq.sort(key=itemgetter(1),reverse=True)


for key,value in wordfreq:
    f.write(key+"\t"+str(value)+"\n")

f.close()
print "Done"




