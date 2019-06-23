# copy the following command lines into test.sh
set -xe

export DATABASE="VOLT"

# compile stored procedures
cd ~/hadoop/voltdb && bash clean_procedures.sh
cd .. && javac HdfsMetaInfoSchema.java && java HdfsMetaInfoSchema
cd ~/hadoop/voltdb && bash create_procedures.sh

# restart hadoop hdfs
cd $HADOOP_HOME
kill $(jps | grep '[NameNode,DataNode]' | awk '{print $1}') || true
rm -rf ~/hadoop/data/*
rm -rf ~/hadoop/name/*
rm -rf ~/hadoop/tmp/*
rm -rf logs/*
./bin/hdfs namenode -format
./sbin/start-dfs.sh
