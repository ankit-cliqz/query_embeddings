#!/usr/local/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ankit'
import sys
import re
import string

removelist = "\n. äöüß€"
rgx = re.compile('[^\w'+removelist+']', re.UNICODE)


if len(sys.argv) != 3:
    print 'Usage: python preprocess_corpusfile.py <input_file> <output_file>'
    sys.exit()

inputfilepath = sys.argv[1]
outputfilepath = sys.argv[2]
fw = open(outputfilepath, 'a')

def remove_non_alphanumchars(line):
    #removelist = "=."
    removelist = "\n. äöüß€"
    #mystring = "asdfADBuUuÜ1234=.!@#$"
    #out_line = re.sub(r'[^\w'+removelist+']', '',line)
    out_line = re.sub(rgx, '',line)
    return out_line

def preprocess_line(line):
    # Lowercase String
    # line = line.decode("utf-8").lower()
    # Lower case special German Characters
    #line=line.replace("Ä","ä").replace("Ö","ö").replace("Ü","ü").replace("ẞ","ß")
    # Replace full stop with Spaces around the full stop
    line =line.replace("." , " . ")
    # Handling Apostrophe s and its variations
    #apostrophes = ["'s","‘s","’s","‛s"]
    #for aps in apostrophes:
    #    line =line.replace(aps , "s")
    # Not included in the preliminary list : Escaped Single Quotes: "'","‘","’","‛",
    # Replace charachters in the charachter list with spaces
    replace_char_list= ["”","''","=","-","—","–","«","…","(",")","\"","\'","\'","„","“",",",":",";","?","<",">","_","+","!","^","*","/", "|","`","~","{","}","[","]"]
    for character in replace_char_list:
        line = line.replace(character, " ")

    line = ' '.join([remove_non_alphanumchars(x.strip().decode("utf-8").lower()) for x in line.split(" ")])+"\n"

    # Remove all other non-alphanumeric characters
    #line  = remove_non_alphanumchars(line)
    # Handling  Contiguous White Spaces
    #line = line.replace("    "," ").replace("   "," ").replace("  "," ").encode("utf-8")
    return line.encode("utf-8")

with open(inputfilepath) as fo:
    for line in fo:
        line_pre =  preprocess_line(line)
        fw.write(line_pre)

print "Input text file pre-processing is complete!"
fo.close()
fw.close()