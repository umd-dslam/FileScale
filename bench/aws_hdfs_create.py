import sys
from fabric import Connection
from tee import StdoutTee, StderrTee

cnn = "3.89.124.4"

node = {"1":"3.89.124.4"}

connect_kwargs = {"key_filename":['/Users/liaogang/.ssh/voltfs.pem']}


with Connection(host=cnn, user="ec2-user", connect_kwargs=connect_kwargs) as c:
	c.run("cd /home/ec2-user/hdfs/hadoop-3.3.0-SNAPSHOT;"
		"source etc/hadoop/hadoop-env.sh;"
		"bash bench.sh")
