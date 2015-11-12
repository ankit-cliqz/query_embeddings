__author__ = 'ankit'
import sys
import json
import requests
import gevent
from gevent import monkey
monkey.patch_all()

itemcount =0


def print_all_info(url):
    response = requests.get("http://10.10.24.254/api/info-page?url=" + url, auth=('cliqz', 'cliqz-245'))
    response_json = json.loads(response.text.decode("utf-8"))
    if not response_json["info"] == "not in index":
        print "\nURL : " + url

        # Related Queries with the URL
        '''
        qc = response_json["info"]["tq"]["qc"]
        if (not len(qc)==0):
            for itemqc in qc:
                print itemqc[0]

        qs =  response_json["info"]["tq"]["qs"]
        if (not len(qs)==0):
            for itemqs in qs:
                print itemqs[0]

        ucrawl =  response_json["info"]["tq"]["ucrawl"]
        if (not len(ucrawl)==0):
            for itemucrawl in ucrawl:
                print itemucrawl[0]
        '''

        flag = False
        # Title and Description
        if (not response_json["snippets"] == "no snippet info"):
            if (not response_json.get("snippets") is None) and (
            not response_json["snippets"].get("language") is None):
                language_dict  = response_json["snippets"]["language"]
                for language, score in language_dict.iteritems():
                    if (language.encode("utf-8")=="de" or language.encode("utf-8") =="en"):
                        flag =True
                        print "Language: "+str(language.encode("utf-8"))
                        break

                if (flag==True):
                    if (not response_json.get("snippets") is None) and (
                    not response_json["snippets"].get("snippet") is None) and (
                    not response_json["snippets"]["snippet"].get("title") is None):
                        title = response_json["snippets"]["snippet"]["title"].encode("utf-8")
                        print "Title: " + str(title)
                    if (not response_json.get("snippets") is None) and (
                    not response_json["snippets"].get("snippet") is None) and (
                    not response_json["snippets"]["snippet"].get("desc") is None):
                        desc = response_json["snippets"]["snippet"]["desc"].encode("utf-8")
                        print "Description: " + str(desc)


# Extract URL FROM Query Logs
with open("/ebsnew/part-00000") as filehandle:
    for query_record in filehandle:
        itemcount += 1
        if itemcount == 2000:
            break
        #print "Currently processing record: " + str(itemcount)

        single_record = json.loads(query_record.decode("utf-8"))

        url =  str(single_record["url"].encode("utf-8"))
        if not url.endswith("/"):
            url = url+"/"

        #print_all_info("dl5.9minecraft.net/index.php?act=dl&id=1375097937/")
        print_all_info(url)
filehandle.close()
