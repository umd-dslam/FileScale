import os

print "Threads\tFiles\tOps/Sec"
for k in [1, 4, 8, 16, 32, 64]:
    for j in [1, 10, 100, 1000, 10000, 100000]:
        filename = "./hopsfs_mkdirs_" + str(k) + "_" + str(j) + ".txt";
        if os.path.isfile(filename):
            f = open(filename, "r")
            searchlines = f.readlines()
            f.close()

            sum = 0.0
            iter = 0
            for i, line in enumerate(searchlines):
                if "Ops per sec: " in line: 
                    iter = iter + 1
                    sum += float(line[line.rfind(':')+1:]) 
                    # print float(line[line.rfind(':')+1:])
            if iter != 0:
                print str(k) + "\t" + str(j) + "\t" + str(sum / iter)
        else:
            continue
