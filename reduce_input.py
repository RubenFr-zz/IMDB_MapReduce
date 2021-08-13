fd = open("InputFiles/names.tsv", 'r')
princ = open("InputFiles/principals1000.tsv", 'r')
writer = open("InputFiles/names1000.tsv", 'w')
i = 0
dic = set()
for line in princ:
	if i == 0:
		i = 1
		continue
	dic.add(line.split("\t")[2])
	
for line in fd:
	if line.split("\t")[0] in dic:
		writer.write(line)
	

