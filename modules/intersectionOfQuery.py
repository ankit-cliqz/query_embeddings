__author__ = 'ankit'



ankitfile = "/ebsnew/w2v_queries/ankit.txt"
allqueriesfile = "/ebsnew/w2v_queries/allqueriesoutput.txt"

query_data_file = open("/ebsnew/w2v_queries/querydata.txt", 'w')


set1 = set()
set2 = set()


with open(ankitfile) as f1:
    for line1 in f1:
        if len(line1.split(" "))>=2:
            set1.add(line1.strip())
f1.close()



with open(allqueriesfile) as f2:
    for line2 in f2:
        set2.add(line2.strip())
f2.close()


setfinal = set1.union(set2)


for line in setfinal:
    query_data_file.write(line+"\n")

query_data_file.close()

