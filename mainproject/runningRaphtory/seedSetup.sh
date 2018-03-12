#!/usr/bin/env bash 
 
IP="$(./getMyIP.sh)" 
 
(docker ps -aq --no-trunc | xargs docker rm) >/dev/null
 
ZooKeeper="161.23.245.235:2181" 
 
LAMName="testLam" 
 
Image="quay.io/miratepuffin/cluster" #if you want to use prebuilt one on my quay.io 
 
NumberOfPartitions=1
 
if [ ! -d logs ]; then mkdir logs; fi 
rm -r logs/seedSetup
if [ ! -d logs/seedSetup ]; then mkdir logs/seedSetup; fi 
chmod 777 logs 
chmod 777 logs/seedSetup
logsdir=$(pwd)"/logs/seedSetup/" 
 
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
 
(docker run --name="lam"  -p $LiveAnalysisPort:$LiveAnalysisPort  --rm -e "BIND_PORT=$LiveAnalysisPort" -e "HOST_IP=$IP" -e "HOST_PORT=$LiveAnalysisPort" $Image LiveAnalysisManager $NumberOfPartitions $ZooKeeper $LAMName &) > logs/seedSetup/LiveAnalysisManager.txt 
sleep 1 
echo "Live Analyser running at $IP:$LiveAnalysisPort" 
 
