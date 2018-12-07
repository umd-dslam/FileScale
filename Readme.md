## Design Doc

### Install PostgreSQL in Docker

Build an image from the Dockerfile and assign it a name.

```bash
docker build -t eg_postgresql .
```

Run the PostgreSQL server container (in the background):

```bash
docker run -d -P --name pg_test eg_postgresql
PORT=$(docker ps -f name=pg_test --format "{{.Ports}}" | sed -e 's/.*://' | awk -F'[->]' '{print $1}')
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
psql -h localhost -p ${PORT} -d docker -U docker
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
# mvn package -Pdist -Pnative -Dtar -DskipTests
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

- [ ] Analyse INode, INodeFile, and INodeDirectory classes

- [ ] Replace them by DB Table

- [ ] Avoid update INode Operations into FSEditLog

- [x] Integrate PostgreSQL

- [ ] [NNThroughputBenchmark](http://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/Benchmarking.html)

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
Test


=====================

replace Postgres by Calvin
```




### References

1. MVN: https://radio123.iteye.com/blog/1490335
2. PostgreSQL: https://www.digitalocean.com/community/tutorials/how-to-install-and-use-postgresql-on-ubuntu-16-04