#!/usr/bin/env bash 
 
IP="$(./getMyIP.sh)" 
 
ZooKeeper="161.23.245.190:2181,161.23.245.190:2182,161.23.245.190:2183" 
 
Image="quay.io/miratepuffin/cluster" #if you want to use prebuilt one on my quay.io 
 
NumberOfPartitions=5
 
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
UpdatePort=9103 
BenchmarkPort=9104 
 
LiveAnalysisPort=9105 
 
ClusterUpPort=9106 
 
(docker run -p $SeedPort:2551 --rm -e "HOST_IP=$IP" -e "HOST_PORT=$SeedPort" $Image seed $IP:$SeedPort $ZooKeeper &) > logs/seedSetup/seed.txt 
sleep 5 
echo "Seed node up and running at $IP:$SeedPort" 
 
(docker run -p $RestPort:2551 -p 8080:8080 --rm -e "HOST_IP=$IP" -e "HOST_PORT=$RestPort" $Image rest $ZooKeeper &) > logs/seedSetup/rest.txt 
sleep 1 
echo "REST API node up and running at $IP:$RestPort" 
 
(docker run -p $UpdatePort:2551  --rm -e "HOST_IP=$IP" -e "HOST_PORT=$UpdatePort" $Image updateGen $NumberOfPartitions $ZooKeeper &) > logs/seedSetup/updateGenerator.txt 
sleep 1 
echo "Update Generator up and running at $IP:$UpdatePort" 
 
(docker run -p $BenchmarkPort:2551  --rm -e "HOST_IP=$IP" -e "HOST_PORT=$BenchmarkPort" $Image benchmark $NumberOfPartitions $ZooKeeper &) > logs/seedSetup/benchmark.txt 
sleep 1 
echo "Benchmarker up and running at $IP:$BenchmarkPort" 
 
(docker run -p $LiveAnalysisPort:2551  --rm -e "HOST_IP=$IP" -e "HOST_PORT=$LiveAnalysisPort" $Image LiveAnalysisManager $NumberOfPartitions $ZooKeeper &) > logs/seedSetup/LiveAnalysisManager.txt 
sleep 1 
echo "Live Analyser running at $IP:$LiveAnalysisPort" 
 
