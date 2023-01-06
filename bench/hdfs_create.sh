#! /bin/bash

for ((i=4;i<=64;i=i*2))
do
	for ((j=1;j<=1000000;j=j*10))
	do
		for k in {1..2}
		do
        ./sbin/stop-dfs.sh

        # restart hadoop hdfs
        rm -rf ~/hadoop/data/*
        rm -rf ~/hadoop/name/*
        rm -rf ~/hadoop/tmp/*
        rm -rf logs/*
        ./bin/hdfs namenode -format -force
        ./sbin/start-dfs.sh

        sleep 10
		./bin/hadoop org.apache.hadoop.hdfs.server.namenode.NNThroughputBenchmark -op create -threads ${i} -files ${j} -filesPerDir 10000000 -keepResults -logLevel INFO &>> hdfs_create_${i}_${j}.txt
		done
	done
done
