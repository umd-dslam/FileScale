rm -rf ~/hadoop/data/*
rm -rf ~/hadoop/name/*
rm -rf ~/hadoop/tmp/*
rm -rf logs/*

PGPASSWORD=docker psql -h localhost -p 5432 -d docker -U docker --command "drop table inodes, inode2block, datablocks, blockstripes, block2storage, storage;"
kill $(jps | grep '[NameNode,DataNode]' | awk '{print $1}') 
./bin/hdfs namenode -format
./sbin/start-dfs.sh
