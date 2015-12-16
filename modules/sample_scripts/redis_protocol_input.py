# Creates the input for the redis-tools script which generates data in redis protocol
fw = open("data.txt", "w")
ri_list = ["ri1.txt", "ri2.txt",  "ri3.txt",   "ri4.txt",  "ri5.txt"]
input_dir = "/run/shm/"
count = 0
for ri in ri_list:
	fr = open (input_dir + ri, "r")
	redis_prefix = ""
	if (ri == "ri1.txt"):
		redis_prefix = "qvin1:"
	elif (ri == "ri2.txt"):
		redis_prefix = "qvin2:"
	elif (ri == "ri3.txt"):
		redis_prefix = "qvin3:"
	elif (ri == "ri4.txt"):
		redis_prefix = "qvin4:"
	elif (ri == "ri5.txt"):
		redis_prefix = "qvin5:"

	for line in fr:
		if (count%100000 == 0):
			print "Completed: " + str(count)

		component = line.split("\t")
		output = "SET "+redis_prefix+component[0]+" "+"\""+component[1]"\""+"\n"
		fw.write(output)
		count+=1

fw.close()

