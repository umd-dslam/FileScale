# copy the following command lines into test.sh
set -xe

kill $(jps | grep 'NameNode' | awk '{print $1}') || true
kill $(jps | grep 'DataNode' | awk '{print $1}') || true

export DATABASE="VOLT"

# compile stored procedures
cd ~/hadoop/voltdb && bash clean_procedures.sh
cd .. && javac HdfsMetaInfoSchema.java && java HdfsMetaInfoSchema
cd ~/hadoop/voltdb && bash create_procedures.sh

# restart hadoop hdfs
cd $HADOOP_HOME
rm -rf ~/hadoop/data/*
rm -rf ~/hadoop/name/*
rm -rf ~/hadoop/tmp/*
rm -rf logs/*
./bin/hdfs namenode -format
./sbin/start-dfs.sh
