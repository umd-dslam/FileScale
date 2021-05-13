# copy the following command lines into test.sh
set -xe

./sbin/stop-dfs.sh

export DATABASE="IGNITE"

if [ -z "$1" ]
then
    IP="localhost"
else
    IP=$1
fi

javac HdfsMetaInfoSchema.java && java HdfsMetaInfoSchema

# restart hadoop hdfs
cd $HADOOP_HOME
rm -rf ~/hadoop/data/*
rm -rf ~/hadoop/name/*
rm -rf ~/hadoop/tmp/*
rm -rf logs/*
./bin/hdfs namenode -format -force
./sbin/start-dfs.sh
