## Design Doc

### Install PostgreSQL in Docker

Build an image from the Dockerfile and assign it a name.

```bash
docker build -t eg_postgresql .
```

Run the PostgreSQL server container (in the background):

```bash
docker run -d -p 5432:5432 --name pg_test eg_postgresql
# PORT=$(docker ps -f name=pg_test --format "{{.Ports}}" | sed -e 's/.*://' | awk -F'[->]' '{print $1}')
```


### Build Hadoop in another Docker

1. Build Hadoop Development Environment Docker

```bash
# Install Docker
# https://docs.docker.com/docker-for-mac/install/

# Build Hadoop Build Environment [Docker]
./start-build-env.sh

# Jump into container
docker exec -it hadoop-dev bash
```


2. Interact with Postgres via Hadoop container

```bash
psql -h localhost -p 5432 -d docker -U docker
# password

docker=# CREATE TABLE cities (name            varchar(80),location        point);
docker=# INSERT INTO cities VALUES ('San Francisco', '(-194.0, 53.0)');
docker=# select * from cities;

     name      | location
---------------+-----------
 San Francisco | (-194,53)
(1 row)
```


3. Build HDFS Source Code via Hadoop container

```
# Build Hadoop in Docker
USER=$(ls /home/)
chown -R $USER /home/$USER/.m2
cd hadoop-hdfs-project

# Compile HDFS
mvn clean install -DskipTests
# Resume build
mvn clean install -DskipTests -rf :hadoop-hdfs
# mvn package -Pdist -Pnative -Dtar -DskipTests
```

### Simplify Compiling Process

```bash
cd hadoop/hadoop-hdfs-project/hadoop-hdfs/
mvn clean package -Pdist -DskipTests

cp  hadoop-hdfs/target/hadoop-hdfs-3.3.0-SNAPSHOT.jar ../hadoop-dist/target/hadoop-3.3.0-SNAPSHOT/share/hadoop/hdfs/

cp  hadoop-hdfs/target/hadoop-hdfs-3.3.0-SNAPSHOT-tests.jar ../hadoop-dist/target/hadoop-3.3.0-SNAPSHOT/share/hadoop/hdfs/
```

### Benchmark

1. add hostname as an **alias** of localhost in `/etc/hosts`

```bash
# set password
$ sudo passwd gangl
$ sudo passwd root

$ cat /etc/hostname

$ sudo vim /etc/hosts 
127.0.0.1       localhost linuxkit-025000000001
```

2. change the `core_site.xml` and `hdfs_site.xml` in `$HADOOP_HOME/etc/`

```bash
# check ip
/sbin/ifconfig eth0 | grep 'inet addr' | cut -d: -f2 | awk '{print $1}'
```

```bash
# mkdir name data
mkdir -p $HOME/hadoop/tmp
mkdir -p $HOME/hadoop/name
mkdir -p $HOME/hadoop/data
```

```bash
# core_site.xml
	<configuration>
	    <property>
	        <name>fs.defaultFS</name>
	        <value>hdfs://192.168.65.3:9000</value>
	    </property>
	    
	    <property>
	        <name>hadoop.tmp.dir</name>
	        <value>/home/gangl/hadoop/tmp</value>
	    </property>
	</configuration>
```

```bash
	<configuration>
		<property>
			<name>dfs.replication</name>
			<value>1</value>
		</property>

		<property>
			<name>dfs.namenode.name.dir</name>
			<value>/home/gangl/hadoop/name</value>
		</property>

		<property>
			<name>dfs.datanode.data.dir</name>
			<value>/home/gangl/hadoop/data</value>
		</property>
		<property>
		  <name>dfs.namenode.fs-limits.min-block-size</name>
		  <value>10</value>
		</property>
		<property>
	        <name>dfs.webhdfs.enabled</name>
	        <value>true</value>
	        <description>提供web访问hdfs的权限</description>
	    </property>
	</configuration>
```

3. ssh

```bash
ssh-keygen -t rsa -f ~/.ssh/id_dsa  
cat ~/.ssh/id_dsa.pub >> ~/.ssh/authorized_keys  
chmod 0600 ~/.ssh/authorized_keys
```

4. start namenode

```bash
cd $HADOOP_HOME

# add env variables into hadoop-env.sh
export HADOOP_ROOT_LOGGER=INFO,console
export HADOOP_CLASSPATH="/home/gangl/java/postgresql-42.2.5.jar:"

./bin/hdfs namenode -format
# add JAVA_HOME to sudo vim /etc/environment
sudo vim /etc/environment
JAVA_HOME="/usr/lib/jvm/java-1.8.0-openjdk-amd64"

sudo vim ./etc/hadoop/hadoop-env.sh
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64

service ssh restart

kill $(jps | grep '[NameNode,DataNode]' | awk '{print $1}')
./sbin/start-dfs.sh


IP=$(/sbin/ifconfig eth0 | grep 'inet addr' | cut -d: -f2 | awk '{print $1}')
./bin/hadoop org.apache.hadoop.hdfs.server.namenode.NNThroughputBenchmark -fs hdfs://${IP}:9000 -op open -threads 1 -files 1 -keepResults -logLevel INFO
```

### Restart

```bash
cd $HADOOP_HOME
rm -rf ~/hadoop/data/*
rm -rf ~/hadoop/name/*
rm -rf ~/hadoop/tmp/*
rm -rf logs/*

kill $(jps | grep '[NameNode,DataNode]' | awk '{print $1}')


./bin/hdfs namenode -format
./sbin/start-dfs.sh

./bin/hadoop org.apache.hadoop.hdfs.server.namenode.NNThroughputBenchmark -fs hdfs://${IP}:9000 -op open -threads 1 -files 100000 -keepResults -logLevel INFO


# open  *
./bin/hadoop org.apache.hadoop.hdfs.server.namenode.NNThroughputBenchmark -fs hdfs://${IP}:9000 -op open -threads 100 -files 100 -keepResults -logLevel INFO

# create *
./bin/hadoop org.apache.hadoop.hdfs.server.namenode.NNThroughputBenchmark -fs hdfs://${IP}:9000 -op create -threads 1 -files 2 -keepResults -logLevel INFO

# delete *
./bin/hadoop org.apache.hadoop.hdfs.server.namenode.NNThroughputBenchmark -fs hdfs://${IP}:9000 -op delete -threads 1 -files 10 -keepResults -logLevel INFO

# mkdirs *
./bin/hadoop org.apache.hadoop.hdfs.server.namenode.NNThroughputBenchmark -fs hdfs://${IP}:9000 -op mkdirs -threads 1 -dirs 10 -keepResults -logLevel INFO
```


### References

1. MVN: https://radio123.iteye.com/blog/1490335
2. PostgreSQL: https://www.digitalocean.com/community/tutorials/how-to-install-and-use-postgresql-on-ubuntu-16-04
