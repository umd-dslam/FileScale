
```bash
docker run -d -p 10800:10800 -p 47500:47500 -p 49112:49112 -p 11211:11211 -v ${PWD}/work_dir:/storage  -e IGNITE_WORK_DIR=/storage  -v ${PWD}/config/ignite-config.xml:/config-file.xml  -e CONFIG_URI=/config-file.xml apacheignite/ignite

mvn compile
export DATABASE="IGNITE"
mvn exec:java -Dexec.mainClass=HdfsMetaInfoSchema -DIGNITE_REST_START_ON_CLIENT=true
```