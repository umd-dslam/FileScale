import sys
from fabric import Connection
from tee import StdoutTee, StderrTee

cnn = "54.210.61.195"

node = {"1":"54.210.61.195",
		"2":"18.207.218.241",
        "3":"3.80.206.89",
        "4":"3.86.187.51",
        "5":"34.227.14.189",
        "6":"100.26.112.54",
        "7":"35.175.146.118",
        "8":"54.91.147.7"}

connect_kwargs = {"key_filename":['/Users/liaogang/.ssh/voltfs.pem']}

for key, value in node.items():
	if int(key) >= 5:
		continue
	print(value)
	with Connection(host=value, user="ec2-user", connect_kwargs=connect_kwargs) as c:
		c.run("cd /home/ec2-user/voltfs/hadoop-3.3.0-SNAPSHOT;"
			"source etc/hadoop/hadoop-env.sh;"
			"bash test.sh")

with StderrTee("aws_voltfs_create_2nn1.txt"), Connection(host=node["2"], user="ec2-user", connect_kwargs=connect_kwargs) as c:
	c.run("cd /home/ec2-user/voltfs/hadoop-3.3.0-SNAPSHOT;" +
		"source etc/hadoop/hadoop-env.sh;" +
		"./bin/hadoop org.apache.hadoop.hdfs.server.namenode.NNThroughputBenchmark " +
		"-fs hdfs://localhost:9000 -op create -threads 1 -files 1000000 -filesPerDir 1000005 " +
		"-keepResults -logLevel INFO")
