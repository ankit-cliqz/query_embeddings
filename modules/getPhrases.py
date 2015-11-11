__author__ = 'ankit'

from itertools import tee, izip
import nltk
import time

'''
NOTE: Assuming the query is preprocessed, before invoking this function.
1. Get the Phrases from the Word Embeddings Corpus
2. Given a query, identify the phrases from the learned phrases and conver the query into phrased query.
-- Identification of phrases with the underscore.

-- Check if the initial query consists of an underscore.

Example:
original query:     where does ruud van nistelrooy play
phrasedquery:       where does ruud_van_nistelrooy play

Counts of the "_" in respective bigram, trigram and quad-gram query.
two_words : 1
three_word_query: 2
four_tmp_word_query: 3
'''

wordvectorslist = ["new_york","new_york_city","new_york_city_fc"]
phrase_set =set(wordvectorslist)

def window(iterable, size):
    iters = tee(iterable, size)
    for i in xrange(1, size):
        for each in iters[i:]:
            next(each, None)
    return izip(*iters)


def get_phrase_combinations(tokens):
    all_possible_phrases = []
    for each in window(tokens, 4):
        all_possible_phrases.append("_".join(list(each)))
    for each in window(tokens, 3):
        all_possible_phrases.append("_".join(list(each)))
    for each in window(tokens, 2):
        all_possible_phrases.append("_".join(list(each)))
    return all_possible_phrases



def getPhrasedQuery(query):
    start  = time.time()
    query_underscored = query.replace(" ","_")
    tokens = nltk.word_tokenize(query)


    words_as_phrase = get_phrase_combinations(tokens)
    matched_phrases = []

    for tmp in words_as_phrase:
        if tmp in phrase_set:
            matched_phrases.append(tmp)

    tmpstring = query
    for phrase in matched_phrases:
        if phrase in query_underscored:
            if phrase.count("_") == 3:
                t1 = phrase.replace("_"," ")
                tmpstring = tmpstring.replace(t1, phrase)
            elif phrase.count("_") == 2:
                t2 = phrase.replace("_"," ")
                tmpstring = tmpstring.replace(t2, phrase)
            elif phrase.count("_") == 1:
                t3 = phrase.replace("_"," ")
                tmpstring = tmpstring.replace(t3, phrase)
    end  = time.time()
    print "Time Taken: "+str(end-start)+" seconds"
    return tmpstring


query1 = "This is new york city general hospital"
print str("Orignial Query: "+query1);
print str(getPhrasedQuery(query1.lower())+"\n")

query2 = "we are playing at new york city fc stadium"
print str("Orignial Query: "+query2);
print str(getPhrasedQuery(query2.lower())+"\n")

query3 = "This is the reason i love new york"
print str("Orignial Query: "+query3);
print str(getPhrasedQuery(query3.lower())+"\n")


query4 = "This is the reason i love new york . new york is simply the best place in the world"
print str("Orignial Query: "+query4);
print str(getPhrasedQuery(query4.lower())+"\n")



