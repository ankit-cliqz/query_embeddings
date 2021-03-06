#!/usr/local/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ankit'
import sys
import re
from mapreduce.process_html_data import ProcessHTMLContent
import string
import json
import multiprocessing
from textwrap import dedent
from itertools import izip_longest
import os

removelist = "\n. äöüß€"
rgx = re.compile('[^\w'+removelist+']', re.UNICODE)
multiple_dots_pattern = re.compile(r'(\.+)')
multiple_space_pattern = re.compile(r'(\s+)')
invalid_escape = re.compile(r'\\[0-7]{1,3}')  # up to 3 digits for byte values up to FF

if len(sys.argv) != 5:
    print 'Usage: python preprocess_corpusfile.py <input_file> <output_file> <num_processes> <num_file_chunks>'
    sys.exit()

inputfilepath = sys.argv[1]
outputfilepath = sys.argv[2]
num_processes = int(sys.argv[3])
num_chunks = int(sys.argv[4])
fw = open(outputfilepath, 'a')
count =0

def remove_non_alphanumchars(line):

    #removelist = "\n. äöüß€"
    #out_line = re.sub(r'[^\w'+removelist+']', '',line)
    out_line = re.sub(rgx, '',line)
    return out_line

def repair(line):
    line = unicode(line)
    line.replace("\\u","u").replace("\u","u")
    return line

def grouper(n, iterable, padvalue=None):
	"""grouper(3, 'abcdefg', 'x') -->
	('a','b','c'), ('d','e','f'), ('g','x','x')"""
	return izip_longest(*[iter(iterable)]*n, fillvalue=padvalue)



def preprocess_line(line):
    # Process HTML Content
    pc = ProcessHTMLContent()
    line = pc.process_html_content(unicode(line))
    line = line.replace("\n","").replace("\r","").replace("\t","")

    # Replace the one and more occurrences of '.' with a single 'full_stop'.
    line_dots_rep = re.sub(multiple_dots_pattern,'.', line)
    # Replace full stop with Spaces around the full stop
    line =line_dots_rep.replace("." , " . ")

    # Replace charachters in the charachter list with spaces
    replace_char_list= ["”","''","=","-","—","–","«","…","(",")","\"","\'","\'","„","“",",",":",";","?","<",">","_","+","!","^","*","/", "|","`","~","{","}","[","]"]

    for character in replace_char_list:
        line = line.replace(character, " ")

    line = ' '.join([remove_non_alphanumchars(x.strip().decode("utf-8").lower()) for x in line.split(" ")])
    line = re.sub(multiple_space_pattern, ' ',line)
    return line+"\n"

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
'''
#Multi-Processing Version
inputdata_list= []
with open(inputfilepath) as fo, open("errorlog.txt", "w") as logw:
    for line in fo:
        #print linecount
        if not line.strip() == "":
            inputdata_list.append(line.strip().decode('utf-8'))

# Create pool (p)
p = multiprocessing.Pool(num_processes)
print "\nNumber of Processes: "+str(num_processes)

for chunk in grouper(1000, inputdata_list):
    try:
        results = p.map(preprocess_line, chunk)
        for r in results:
            fw.write(r.encode("utf-8"))
    except:
        pass

print "\nInput text file pre-processing is complete!"
fo.close()
fw.close()
logw.close()

'''
'''
# Single Process Version
with open(inputfilepath) as fo, open("errorlog.txt", "w") as logw:
    for line in fo:
        #print linecount
        if not line.strip() == "":
            try:
                line_pre =  preprocess_line(line.strip().decode('utf-8'))
                #print "Final: "+str(line_pre.encode("utf-8"))
                fw.write(line_pre.encode("utf-8"))
            except:
                fault = line.strip().decode('utf-8')+"\n"
                logw.write(fault.encode("utf-8"))
                pass

print "Input text file pre-processing is complete!"
fo.close()
fw.close()
logw.close()
'''

'''
# File Seek Version for Chunking + MultiProcessing
'''

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
        print "chunk: "+str(chunk)
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

