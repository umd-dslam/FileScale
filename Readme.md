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

### TODO List

- [x] Build HDFS

```bash
[INFO] Reactor Summary:
[INFO]
[INFO] Apache Hadoop HDFS Client .......................... SUCCESS [ 16.388 s]
[INFO] Apache Hadoop HDFS ................................. SUCCESS [ 13.884 s]
[INFO] Apache Hadoop HDFS Native Client ................... SUCCESS [  3.907 s]
[INFO] Apache Hadoop HttpFS ............................... SUCCESS [ 10.136 s]
[INFO] Apache Hadoop HDFS-NFS ............................. SUCCESS [  2.790 s]
[INFO] Apache Hadoop HDFS-RBF ............................. SUCCESS [  3.966 s]
[INFO] Apache Hadoop HDFS Project ......................... SUCCESS [  0.444 s]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 55.759 s
[INFO] Finished at: 2018-12-07T04:54:09+00:00
[INFO] Final Memory: 50M/648M
[INFO] ------------------------------------------------------------------------
````

- [x] create

- [ ] open

- [ ] rename

- [x] Analyse INode, INodeFile, and INodeDirectory classes

- [x] Replace them by DB Table

- [ ] Avoid update INode Operations into FSEditLog

- [x] Integrate PostgreSQL

- [ ] [NNThroughputBenchmark](http://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/Benchmarking.html)

- [ ] Simplify protobuf

- [ ] Simplify readLog

- [ ] Delte updataBlockSMap - FSIamgeFormatPBINode.java

- [ ] Remove write locks

- [ ] Remove copy constructor of INodeFile/INodeDirectory when loading underconstructing

- [ ] Simplify insert logic (use sql return value to check duplicated values)

- [ ] Report


### Data Structure

```bash
Filesystem 

========================
in memory:

INode: INodeDirectory and INodeFile

INode {
	id;
	name;
	fullPathName;    *****
	parent;  *****

	userName;
	groupName;
	fsPermission;
	aclFeature;
	modificationTime;
	accessTime;
	XattrFeature;
	....


	isFile();
	isDirectory();
	isSymlink();
	isRoot();
	...
}


INodeDirectory {
children
}

addChild()
removeChild()


INodeFile {
	// header 64bits
	// 4: storage strategy
	// 12: backup coefficient
	// 48: block size info
	// HeaderFormat handles header
	header
	BlockInfo[] blocks

	class HeaderFormat {
	....
	}
}

BlockInfo {
	// inherit from Block class
	// block <-> file  INodeFile
	// block <-> datanode 
}


Problem: Snapshot ???

==========================
in disk:

replace FSImage and FSEditLog's  INode Section ==> postgres

Adv: No need to sync in-memory data structure (INode) into disk

But FSImage file includes many different meta info:

> NameSystem Section
> **INode Section**
> SnapshotSection
> SecretManager Section
> StringTable Section
> ...

If we can replace all of them, that will be great! but sounds impossible for now.

Focus on (intercepting INode Operations)

in memory FS-----------------
   |                        |
   |                        |
   |                        |
Inode Operation          Snapshot, NameSystem, SecretManager, StringTable, ...
   |                        |
   |                        |
   |                        |
Postgres: Yes          FSimage and FSEditLog: Yes



1. Still need to understand FSEditLog and Stop writing INode Operations into log
**very important**
If INode Op in log, it will still build some in-memory FS. (Waste Memory)


=====================
Bechmark Track code


clientProto.getFileInfo
clientProto.rename
clientProto.getBlockLocations

clientProto.delete ==> FSNamesystem.delete ==> FSDirDeleteOp delete(FSDirectory)

clientProto.create ==> FSNamesystem.startFile ==> .. ==> FSDirWriteFileOp(FSDirectory) ==> addFile ==> addINode

clientProto.complete

clientProto.mkdirs  ===> .. ==> FSDirMkdirOp.mkdirs {createSingleDirectory ...}

clientProto.addBlock  ===> getAdditionalBlock ==> FSDirWriteFileOp.storeAllocatedBlock
clientProto.refreshNodes

Open file statistics: Measure how many open calls (getBlockLocations()) 
the name-node can handle per second.


Minimal data-node simulator


=====================

Table
{
	long id
	byte[] name
	PermissionStatus permissions = {
		String username;
		String groupname;
		FsPermission permission = {
			FsAction useraction = null;  ==> String
			FsAction groupaction = null; ==> String
			FsAction otheraction = null; ==> String
			Boolean stickyBit = false;
		}
	}
	long modificationTime
	long accessTime
	LinkedElement next   ??
	Feature[] features   ??

	# file attributes
	BlockInfo[] blklist
	short replication
	long preferredBlockSize

	# diretory attributes
	List<INode>  children
}

=====================

replace Postgres by Calvin
```




### References

1. MVN: https://radio123.iteye.com/blog/1490335
2. PostgreSQL: https://www.digitalocean.com/community/tutorials/how-to-install-and-use-postgresql-on-ubuntu-16-04
