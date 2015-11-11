#!/usr/local/bin/python
# -*- coding: utf-8 -*-
__author__ = 'ankit'

import re
removelist = "=."
mystring = "asdfADBuUuÜ1234=.!@#$"
out = re.sub(r'[^\w'+removelist+']', '',mystring)

print out