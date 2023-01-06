# copy the following command lines into test.sh
set -xe

cd $HADOOP_HOME
./sbin/stop-dfs.sh

export DATABASE="IGNITE"
export IGNITE_SERVER="172.31.32.188"

if [ -z "$1" ]
then
    IP="localhost"
else
    IP=$1
fi

cd ~/hadoop/filescale_init
mvn compile
mvn exec:java -Dexec.mainClass=HdfsMetaInfoSchema  -DIGNITE_REST_START_ON_CLIENT=true

# restart hadoop hdfs
cd $HADOOP_HOME
rm -rf ~/hadoop/data/*
rm -rf ~/hadoop/name/*
rm -rf ~/hadoop/tmp/*
rm -rf logs/*
./bin/hdfs namenode -format -force
./sbin/start-dfs.sh
# ./bin/hadoop org.apache.hadoop.hdfs.server.namenode.NNThroughputBenchmark -fs hdfs://localhost:9000 -op create -threads 16 -files 10000 -filesPerDir 100000 -keepResults -logLevel INFO