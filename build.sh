# copy the following command lines into build.sh

cd ~/hadoop/hadoop-hdfs-project/hadoop-hdfs-db/
mvn install -Pdist -DskipTests
cp target/hadoop-hdfs-db-1.0.0.jar $HADOOP_HOME/share/hadoop/hdfs/lib/
cd ~/hadoop/hadoop-hdfs-project/hadoop-hdfs/
mvn package -Pdist -DskipTests
cp target/hadoop-hdfs-3.3.0-SNAPSHOT.jar $HADOOP_HOME/share/hadoop/hdfs/
cp target/hadoop-hdfs-3.3.0-SNAPSHOT-tests.jar $HADOOP_HOME/share/hadoop/hdfs/
cd $HADOOP_HOME
