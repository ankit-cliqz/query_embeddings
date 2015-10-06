__author__ = 'ankit'

import os

directory_path = "/ebs/data/en_wiki/extracted/"
outputfile_path = "/ebs/data/en_wiki/en_wiki.txt"

fh = open(outputfile_path, 'a')
directoryCount = 1

for root, dirs, files in os.walk(directory_path):
    print "Directory Count: " + str(directoryCount)
    path = root.split('/')
    for file in files:
        if (file.lower().startswith('wiki_')):
            FilePath = os.path.join(root, str(file))
            tmp = os.path.dirname(FilePath)
            root_dir_name = os.path.basename(tmp)

            wiki_file_fullpath = os.path.join(directory_path, root_dir_name, file)

            with open(wiki_file_fullpath) as f1:
                for tmp in f1:
                    #tmp = line1.encode("utf-8")
                    if not (tmp.startswith("<doc id=") or tmp.startswith("</doc>") or tmp.startswith("\n")):
                        # Open a file handle to the lock file
                        fh.write(str(tmp))
            f1.close()
    directoryCount+=1
fh.close()