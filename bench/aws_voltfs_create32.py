import sys
from fabric import Connection
from tee import StdoutTee, StderrTee

cnn = "3.95.218.253"

node = {"1":"3.95.218.253",
		"2":"3.86.224.36",
        "3":"34.230.74.46",
        "4":"54.174.45.253",
        "5":"35.175.146.114",
        "6":"54.236.178.100",
        "7":"52.91.200.92",
        "8":"35.171.23.86",
		"9":"34.238.119.140",
        "10":"34.238.152.118",
        "11":"34.238.235.121",
        "12":"3.95.8.223",
        "13":"3.82.145.128",
        "14":"18.207.195.249",
        "15":"184.72.102.92",
		"16":"54.87.184.159",
        "17":"34.230.71.202",
        "18":"54.152.88.48",
        "19":"18.204.213.108",
        "20":"54.88.232.184",
        "21":"54.243.12.184",
        "22":"3.85.160.202",
		"23":"3.95.154.153",
        "24":"34.207.167.5",
        "25":"18.212.38.73",
        "26":"100.26.194.178",
        "27":"3.83.87.206",
        "28":"3.89.251.96",
        "29":"3.82.139.86",
		"30":"100.27.19.118",
        "31":"54.144.62.126",
        "32":"3.93.248.58"}

connect_kwargs = {"key_filename":['/Users/liaogang/.ssh/voltfs.pem']}

for key, value in node.items():
	if int(key) >= 3:
		continue
	print(value)
	with Connection(host=value, user="ec2-user", connect_kwargs=connect_kwargs) as c:
		c.run("cd /home/ec2-user/voltfs/hadoop-3.3.0-SNAPSHOT;"
			"source etc/hadoop/hadoop-env.sh;"
			"bash test.sh")

with StderrTee("aws_voltfs_create_2.txt"), Connection(host=cnn, user="ec2-user", connect_kwargs=connect_kwargs) as c:
	c.run("cd /home/ec2-user/voltfs/hadoop-3.3.0-SNAPSHOT;" +
		"source etc/hadoop/hadoop-env.sh;" +
		"./bin/hadoop org.apache.hadoop.hdfs.server.namenode.NNThroughputBenchmark " +
		"-fs hdfs://localhost:65212 -op create -threads 2 -files 2000000 -filesPerDir 1000005 " +
		"-keepResults -logLevel INFO")
