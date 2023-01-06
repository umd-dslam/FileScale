import sys
from fabric import Connection
from tee import StdoutTee, StderrTee

cnn = "34.203.247.95"

node = {"1":"34.203.247.95"}

connect_kwargs = {"key_filename":['/Users/liaogang/.ssh/voltfs.pem']}


with Connection(host=cnn, user="ec2-user", connect_kwargs=connect_kwargs) as c:
	c.run("cd /home/ec2-user/hdfs/hadoop-3.3.0-SNAPSHOT;"
		"source etc/hadoop/hadoop-env.sh;"
		"bash bench.sh")
