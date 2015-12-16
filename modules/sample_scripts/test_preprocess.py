#!/usr/local/bin/python
# -*- coding: utf-8 -*-

__author__ = 'ankit'
import re
import string
def preprocess_line(line):
    for word in line:
        preprocess_word(word)
    pattern = re.compile('[\W_]+')
    line_tmp = pattern.sub('', line)
    return line_tmp

def preprocess_word(word):
    pattern = re.compile('[\W_]+')
    word_tmp = pattern.sub('', word)
    return word_tmp

str = "if you accidentally slide past the base you don't get called out of the baseline,\" he said, adding that qualified in this case.damage to both ships is being evaluated, with both ships currently operating under their own power. next year it will also replace its douro river ship in portugal, which stops in porto and the wine-growing town of pinhão, with the 118-passenger queen isabel. he tried to cross the street."


print preprocess_line(str)