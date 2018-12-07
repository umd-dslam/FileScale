## Design Doc

### Build Hadoop

```bash
# Install Docker
# https://docs.docker.com/docker-for-mac/install/

# Build Hadoop Build Environment [Docker]
./start-build-env.sh

# Build Hadoop in Docker
USER=$(ls /home/)
sudo chown -R $USER /home/$USER/.m2
cd hadoop-hdfs-project
mvn clean install -DskipTests
# mvn package -Pdist -Pnative -Dtar -DskipTests
```