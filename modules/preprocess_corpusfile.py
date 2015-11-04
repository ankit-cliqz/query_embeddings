#!/usr/local/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ankit'
import sys
import re
import string
if len(sys.argv) != 3:
    print 'Usage: python preprocess_corpusfile.py <input_file> <output_file>'
    sys.exit()

inputfilepath = sys.argv[1]
outputfilepath = sys.argv[2]
fw = open(outputfilepath, 'a')


def preprocess_line(line):
    replace_char_list= ["(",")","\""," \'","\' ","„","“",",",":",";","?","<",">","_","+","!","^","*","/", "|","`","~","{","}","[","]","   ","  "]
    line =line.replace("." , " . ")
    line =line.replace("- " , " ")

    for character in replace_char_list:
        line = line.replace(character, " ")
    return line

with open(inputfilepath) as fo:
    for line in fo:
        line_pre =  preprocess_line(line)
        fw.write(line_pre)

print "preprocess_corpusfile.py Done!"
fo.close()
fw.close()