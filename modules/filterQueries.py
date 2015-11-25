import sys

if len(sys.argv) != 3:
    print 'Usage: python filterQueries.py <input_file> <output_file>'
    sys.exit()

inputfilepath = sys.argv[1]
outputfilepath = sys.argv[2]

fw = open(outputfilepath, "w")
print "Starting ..."
with open(inputfilepath) as fo:
    for line in fo:
        if not line.strip() == "":
            line = line.strip().decode("utf-8")
            components = line.split(" ")
            if len(components)>2:
                fw.write(line.encode("utf-8")+"\n")
fo.close()
fw.close()

print "Done!"


