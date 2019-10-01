# copy the following command lines into test.sh
set -xe

./sbin/stop-dfs.sh

export DATABASE="VOLT"

if [ -z "$1" ]
then
    IP="localhost"
else
    IP=$1
fi

# compile stored procedures
cd ~/hadoop/voltdb && bash clean_procedures.sh $IP
cd .. && javac HdfsMetaInfoSchema.java && java HdfsMetaInfoSchema
cd ~/hadoop/voltdb && bash create_procedures.sh $IP

# restart hadoop hdfs
cd $HADOOP_HOME
rm -rf ~/hadoop/data/*
rm -rf ~/hadoop/name/*
rm -rf ~/hadoop/tmp/*
rm -rf logs/*
./bin/hdfs namenode -format
./sbin/start-dfs.sh
