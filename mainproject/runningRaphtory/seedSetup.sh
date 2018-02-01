#!/usr/bin/env bash 
 
IP="$(./getMyIP.sh)" 
 
docker ps -aq --no-trunc | xargs docker rm
 
ZooKeeper="138.37.32.88:2181" 
 
LAMName="testLam" 
 
Image="quay.io/miratepuffin/cluster" #if you want to use prebuilt one on my quay.io 
 
NumberOfPartitions=1
 
NumberOfUpdates=0
 
JVM="-Dcom.sun.management.jmxremote.rmi.port=9090 -Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.port=9090  -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.local.only=false -Djava.rmi.server.hostname=$IP" 
if [ ! -d logs ]; then mkdir logs; fi 
rm -r logs/seedSetup
if [ ! -d logs/seedSetup ]; then mkdir logs/seedSetup; fi 
if [ ! -d logs/seedSetup/entityLogs/ ]; then mkdir logs/seedSetup/entityLogs/; fi 
entityLogs=$(pwd)"/logs/seedSetup/entityLogs" 
 
chmod 777 logs 
chmod 777 logs/seedSetup
chmod 777 logs/seedSetup/entityLogs
 
SeedPort=9101 
RestPort=9102 
BenchmarkPort=9104 
 
LiveAnalysisPort=9105 
 
(docker run --name="seednode" -p $SeedPort:$SeedPort --rm -e "BIND_PORT=$SeedPort" -e "HOST_IP=$IP" -e "HOST_PORT=$SeedPort" $Image seed $IP:$SeedPort $ZooKeeper &) > logs/seedSetup/seed.txt 
sleep 5 
echo "Seed node up and running at $IP:$SeedPort" 
 
(docker run --name="rest"  -p $RestPort:$RestPort -p 8080:8080 --rm -e "BIND_PORT=$RestPort" -e "HOST_IP=$IP" -e "HOST_PORT=$RestPort" $Image rest $ZooKeeper &) > logs/seedSetup/rest.txt 
sleep 1 
echo "REST API node up and running at $IP:$RestPort" 
 
(docker run --name="benchmarker"  -p $BenchmarkPort:$BenchmarkPort  --rm -e "BIND_PORT=$BenchmarkPort" -e "HOST_IP=$IP" -e "HOST_PORT=$BenchmarkPort" $Image benchmark $NumberOfPartitions $ZooKeeper &) > logs/seedSetup/benchmark.txt 
sleep 1 
echo "Benchmarker up and running at $IP:$BenchmarkPort" 
 
