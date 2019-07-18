#!/bin/bash

set -e

git clone https://github.com/hopshadoop/hops-metadata-dal
cd hops-metadata-dal && git checkout master && mvn clean install -DskipTests
cd ..

wget https://bbc1.sics.se/archiva/repository/Hops/com/mysql/ndb/clusterj-native/7.6.10/clusterj-native-7.6.10-natives-linux.jar
unzip clusterj-native-7.6.10-natives-linux.jar
sudo cp libndbclient.so /usr/lib && rm -rf clusterj-native-7.6.10-natives-linux.jar 

git clone https://github.com/hopshadoop/hops-metadata-dal-impl-ndb
cd hops-metadata-dal-impl-ndb && git checkout master && mvn clean install -DskipTests
cd ..

git clone https://github.com/hopshadoop/hops
cd hops && git checkout master && mvn package -Pdist,native -DskipTests -Dtar
