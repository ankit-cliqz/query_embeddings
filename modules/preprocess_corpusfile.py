#!/usr/local/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ankit'
import sys

if len(sys.argv) != 3:
    print 'Usage: python preprocess_corpusfile.py <input_file> <output_file>'
    sys.exit()

inputfilepath = sys.argv[1]
outputfilepath = sys.argv[2]
fw = open(outputfilepath, 'a')


def preprocess_line(line):
    line_tmp1 = line.replace("(", " ")
    line_tmp2 = line_tmp1.replace(")", " ")
    line_tmp3 = line_tmp2.replace("\"", " ")
    line_tmp4 = line_tmp3.replace("\'", " ")
    line_tmp5 = line_tmp4.replace('„', " ")
    line_tmp6 = line_tmp5.replace('“', " ")
    line_tmp7 = line_tmp6.replace('.', " . ")
    line_tmp8 = line_tmp7.replace('  ', " ")
    line_tmp9 = line_tmp8.replace('   ', " ")
    return line_tmp9

with open(inputfilepath) as fo:
    for line in fo:
        line_pre =  preprocess_line(line)
        fw.write(line_pre)

print "Done!"
fo.close()
fw.close()