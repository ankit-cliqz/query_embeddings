__author__ = 'ankit'
import os

decorpusfiles = "/ebsnew/data/de_corpus/tar_gz_files/sentences/"
outputfile = open("/ebsnew/data/de_corpus/decorpus.txt",'w')

for root, dirs, files in os.walk(decorpusfiles):
    path = root.split('/')
    for file in files:
        if (file.lower().endswith('.txt')):
            filepath = os.path.join(root, str(file))
            with open(filepath) as f:
                for line in f:
                    parts = line.split("\t")
                    outputfile.write(str(parts[1]).strip()+"\n")
            f.close()