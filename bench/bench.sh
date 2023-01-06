#! /bin/bash

# Create Files

for ((i=1;i<=8;i=i*2))
do
	for ((j=1;j<=10000;j=j*10))
	do
		for k in {1..20}
		do
 		# your-unix-command-here
		bash ~/hadoop/test.sh
        sleep 10
 		/home/gangliao/hadoop/hadoop-dist/target/hadoop-3.3.0-SNAPSHOT/bin/hadoop \
            org.apache.hadoop.hdfs.server.namenode.NNThroughputBenchmark \
            -fs hdfs://localhost:9000 -op create -threads ${i} -files ${j} \
            -filesPerDir 100000 -logLevel INFO &>> voltfs_create_${i}_${j}.txt
		done
	done
done


# Open Files

bash ~/hadoop/test.sh
/home/gangliao/hadoop/hadoop-dist/target/hadoop-3.3.0-SNAPSHOT/bin/hadoop \
    org.apache.hadoop.hdfs.server.namenode.NNThroughputBenchmark \
    -fs hdfs://localhost:9000 -op open -threads 1 -files 100000 \
    -filesPerDir 100000 -keepResults -logLevel INFO

for ((i=1;i<=8;i=i*2))
do
	for ((j=1;j<=10000;j=j*10))
	do
		for k in {1..20}
		do
 		# your-unix-command-here
        sleep 10
 		/home/gangliao/hadoop/hadoop-dist/target/hadoop-3.3.0-SNAPSHOT/bin/hadoop \
            org.apache.hadoop.hdfs.server.namenode.NNThroughputBenchmark \
            -fs hdfs://localhost:9000 -op open -threads ${i} -files ${j} \
            -filesPerDir 100000 -keepResults -useExisting -logLevel INFO &>> voltfs_open_${i}_${j}.txt
		done
	done
done


# Delete Files

for ((i=1;i<=8;i=i*2))
do
	for ((j=1;j<=10000;j=j*10))
	do
		for k in {1..20}
		do
 		# your-unix-command-here
		bash ~/hadoop/test.sh
        sleep 10
 		/home/gangliao/hadoop/hadoop-dist/target/hadoop-3.3.0-SNAPSHOT/bin/hadoop \
            org.apache.hadoop.hdfs.server.namenode.NNThroughputBenchmark \
            -fs hdfs://localhost:9000 -op delete -threads ${i} -files ${j} \
            -filesPerDir 100 -logLevel INFO &>> voltfs_delete_${i}_${j}.txt
		done
	done
done
