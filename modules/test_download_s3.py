__author__ = 'ankit'


import boto
import sys, os
from boto.s3.key import Key
import gzip
import json
import requests


LOCAL_PATH = '/ebsnew/querylogs/'
AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''
bucket_name = 'cliqz-data-pipeline'

fwrite = open("/ebsnew/page_info_data/title_and_desc.txt", "a")
def print_all_info(url):
    response_json = json.loads(response.text.decode("utf-8"))
    if not response_json["info"] == "not in index":
        #print "\nURL : " + url

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
                        #print "Language: "+str(language.encode("utf-8"))
                        break

                if (flag==True):
                    if (not response_json.get("snippets") is None) and (
                    not response_json["snippets"].get("snippet") is None) and (
                    not response_json["snippets"]["snippet"].get("title") is None):
                        title = response_json["snippets"]["snippet"]["title"].encode("utf-8")
                        #print "Title: " + str(title)
                        fwrite.write(title+"\n")
                    if (not response_json.get("snippets") is None) and (
                    not response_json["snippets"].get("snippet") is None) and (
                    not response_json["snippets"]["snippet"].get("desc") is None):
                        desc = response_json["snippets"]["snippet"]["desc"].encode("utf-8")
                        #print "Description: " + str(desc)
                        fwrite.write(desc+"\n")

# connect to the bucket
conn = boto.connect_s3(AWS_ACCESS_KEY_ID,
                AWS_SECRET_ACCESS_KEY)
bucket = conn.get_bucket(bucket_name)
# go through the list of files
bucket_list = bucket.list(prefix="work_area/querylog/de/20151021")
print "Starting ... "
for l in bucket_list:
    keyString = str(l.key)
    if (keyString.endswith(".gz")):
        filepathstring = keyString.split("/")
        filename = filepathstring[4]
        #print filename

        # check if file exists locally, if not: download it
        if not os.path.exists(LOCAL_PATH+filename):
            l.get_contents_to_filename(LOCAL_PATH+filename)

        inF = gzip.GzipFile(LOCAL_PATH+filename, 'rb')
        gzfile = inF.read()
        inF.close()
        rawfilename = filename.replace(".gz","")
        rawfile = file(LOCAL_PATH+rawfilename, 'wb')
        rawfile.write(gzfile)
        rawfile.close()

        ## Code to Process each Raw File Individually
        print "\nProcessing File: " + rawfilename

        with open(LOCAL_PATH+rawfilename) as filehandle:
            for query_record in filehandle:
                single_record = json.loads(query_record.decode("utf-8"))
                url =  str(single_record["url"].encode("utf-8"))
                if not url.endswith("/"):
                    url = url+"/"
                print_all_info(url)
                #fwrite.write("\n")
        filehandle.close()

        ## File Processed ##

        ## Now Deleting All files
        print "Deleting File: "+ filename
        os.remove(LOCAL_PATH+filename)
        print "Deleting File: "+ rawfilename
        os.remove(LOCAL_PATH+rawfilename)
#Close the File
fwrite.close()

