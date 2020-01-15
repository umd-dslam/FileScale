f = open("./aws_stderr.txt", "r")
searchlines = f.readlines()
f.close()

sum = 0.0
iter = 0
for i, line in enumerate(searchlines):
    if "Ops per sec: " in line: 
        iter = iter + 1
        sum += float(line[line.rfind(':')+1:]) 
        print float(line[line.rfind(':')+1:])
print sum / iter