# copy the following command lines into test.sh
set -xe

cd $HADOOP_HOME
./sbin/stop-dfs.sh

export DATABASE="VOLT"
export VOLT_SERVER="localhost"

if [ -z "$1" ]
then
    IP="localhost"
else
    IP=$1
fi

# compile stored procedures
cd ~/hadoop/filescale_init/voltdb && bash clean_procedures.sh $IP
cd ../src/main/java && javac HdfsMetaInfoSchema.java && java HdfsMetaInfoSchema
cd ~/hadoop/filescale_init/voltdb && bash create_procedures.sh $IP

# restart hadoop hdfs
cd $HADOOP_HOME
rm -rf ~/hadoop/data/*
rm -rf ~/hadoop/name/*
rm -rf ~/hadoop/tmp/*
rm -rf logs/*
./bin/hdfs namenode -format -force
./sbin/start-dfs.sh
