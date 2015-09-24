
import os
import joblib

xyz_file = "/ebs/output_data/queryurl_frequencydict.pkl"
input_file_path = "/ebs/output_data_new/query_url_frequency.txt"

# Dict to store the Query URL and the Frequency
xyz ={}
print "Start"
fh = open(input_file_path, 'r')
for line in fh:
    component = line.split("\t")
    # Query: URL : Frequency: Source
    xyz[(component[0], component[1])] = int(component[2]), component[3]
print "Done!"
# Dump To pickckle file using joblib
joblib.dump(xyz,xyz_file)
print "File Dumped"
fh.close()