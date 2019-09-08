set -xe

docker run --name=volt1 --hostname=volt1 -d -p 8080:8080 -p 7181:7181 -p 21212:21212 \
    gangliao/voltdb:9.1 /root/voltdb-ent/deploy.py 3 1 volt1

LEADERIP=$(docker inspect --format '{{ .NetworkSettings.IPAddress }}' volt1)

docker run --name=volt2 --hostname=volt2 -d -p 7182:7181  -p 21213:21212 \
    gangliao/voltdb:9.1 /root/voltdb-ent/deploy.py 3 1 $LEADERIP

docker run --name=volt3 --hostname=volt3 -d -p 7183:7181  -p 21214:21212 \
    gangliao/voltdb:9.1 /root/voltdb-ent/deploy.py 3 1 $LEADERIP
