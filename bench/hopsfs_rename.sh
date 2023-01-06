#! /bin/bash

for ((i=32;i<=64;i=i*2))
do
	for ((j=1;j<=100000;j=j*10))
	do
		for k in {1..2}
		do
		./bin/hdfs namenode -dropAndCreateDB
		./bin/hdfs namenode -format
		./sbin/stop-nn.sh

                 sleep 10
		./bin/hadoop org.apache.hadoop.hdfs.server.namenode.NNThroughputBenchmark -op rename -threads ${i} -files ${j} -filesPerDir 10000000 -keepResults -logLevel INFO &>> hopsfs_rename_${i}_${j}.txt
		done
	done
done
