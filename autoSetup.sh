#!/usr/bin/env bash 
 
IP="$(./getMyIP.sh)" 
 
./dockblock.sh 
 
Image="dockerexp/cluster" #local if you build your own from the source files 
 
if [ ! -d logs ]; then mkdir logs; fi 
if [ -d logs/entityLogs/ ]; then rm -r logs/entityLogs/; fi 
mkdir logs/entityLogs/ 
entityLogs=$(pwd)"/logs/entityLogs" 
 
NumberOfPartitions=8
 
SeedPort=9101 
RestPort=9102 
UpdatePort=9103 
BenchmarkPort=9104 
 
PM0Port=2200
PM1Port=2201
PM2Port=2202
PM3Port=2203
PM4Port=2204
PM5Port=2205
PM6Port=2206
PM7Port=2207

PM0ID=0
PM1ID=1
PM2ID=2
PM3ID=3
PM4ID=4
PM5ID=5
PM6ID=6
PM7ID=7

Router0Port=2300
Router1Port=2301
Router2Port=2302
Router3Port=2303


 
(docker run -p $SeedPort:2551 --rm -e "HOST_IP=$IP" -e "HOST_PORT=$SeedPort" $Image seed &) > logs/seed.txt 
sleep 1 
echo "Seed node up and running at $IP:$SeedPort" 
 
(docker run -p $RestPort:2551 -p 8080:8080 --rm -e "HOST_IP=$IP" -e "HOST_PORT=$RestPort" $Image rest $IP:$SeedPort &) > logs/rest.txt 
sleep 1 
echo "REST API node up and running at $IP:$RestPort" 
 
(docker run -p $UpdatePort:2551  --rm -e "HOST_IP=$IP" -e "HOST_PORT=$UpdatePort" $Image updateGen $IP:$SeedPort $NumberOfPartitions &) > logs/updateGenerator.txt 
sleep 1 
echo "Update Generator up and running at $IP:$UpdatePort" 
 
(docker run -p $BenchmarkPort:2551  --rm -e "HOST_IP=$IP" -e "HOST_PORT=$BenchmarkPort" $Image benchmark $IP:$SeedPort $NumberOfPartitions &) > logs/benchmark.txt 
sleep 1 
echo "Benchmarker and running at $IP:$BenchmarkPort" 
 

 
(docker run -p $Router0Port:2551  --rm -e "HOST_IP=$IP" -e "HOST_PORT=$Router0Port" $Image router $IP:$SeedPort $NumberOfPartitions &) > logs/router0.txt 
sleep 1 
echo "Router 0 up and running at $IP:$Router1Port" 
 
(docker run -p $Router1Port:2551  --rm -e "HOST_IP=$IP" -e "HOST_PORT=$Router1Port" $Image router $IP:$SeedPort $NumberOfPartitions &) > logs/router1.txt 
sleep 1 
echo "Router 1 up and running at $IP:$Router1Port" 
 
(docker run -p $Router2Port:2551  --rm -e "HOST_IP=$IP" -e "HOST_PORT=$Router2Port" $Image router $IP:$SeedPort $NumberOfPartitions &) > logs/router2.txt 
sleep 1 
echo "Router 2 up and running at $IP:$Router1Port" 
 
(docker run -p $Router3Port:2551  --rm -e "HOST_IP=$IP" -e "HOST_PORT=$Router3Port" $Image router $IP:$SeedPort $NumberOfPartitions &) > logs/router3.txt 
sleep 1 
echo "Router 3 up and running at $IP:$Router1Port" 
 

 
(docker run -p $PM0Port:2551  --rm -e "HOST_IP=$IP" -e "HOST_PORT=$PM0Port" -v $entityLogs:/logs/entityLogs $Image partitionManager $IP:$SeedPort $PM0ID $NumberOfPartitions &) > logs/partitionManager0.txt 
sleep 1 
echo "Partition Manager $PM0ID up and running at $IP:$PM0Port" 
 
(docker run -p $PM1Port:2551  --rm -e "HOST_IP=$IP" -e "HOST_PORT=$PM1Port" -v $entityLogs:/logs/entityLogs $Image partitionManager $IP:$SeedPort $PM1ID $NumberOfPartitions &) > logs/partitionManager1.txt 
sleep 1 
echo "Partition Manager $PM1ID up and running at $IP:$PM1Port" 
 
(docker run -p $PM2Port:2551  --rm -e "HOST_IP=$IP" -e "HOST_PORT=$PM2Port" -v $entityLogs:/logs/entityLogs $Image partitionManager $IP:$SeedPort $PM2ID $NumberOfPartitions &) > logs/partitionManager2.txt 
sleep 1 
echo "Partition Manager $PM2ID up and running at $IP:$PM2Port" 
 
(docker run -p $PM3Port:2551  --rm -e "HOST_IP=$IP" -e "HOST_PORT=$PM3Port" -v $entityLogs:/logs/entityLogs $Image partitionManager $IP:$SeedPort $PM3ID $NumberOfPartitions &) > logs/partitionManager3.txt 
sleep 1 
echo "Partition Manager $PM3ID up and running at $IP:$PM3Port" 
 
(docker run -p $PM4Port:2551  --rm -e "HOST_IP=$IP" -e "HOST_PORT=$PM4Port" -v $entityLogs:/logs/entityLogs $Image partitionManager $IP:$SeedPort $PM4ID $NumberOfPartitions &) > logs/partitionManager4.txt 
sleep 1 
echo "Partition Manager $PM4ID up and running at $IP:$PM4Port" 
 
(docker run -p $PM5Port:2551  --rm -e "HOST_IP=$IP" -e "HOST_PORT=$PM5Port" -v $entityLogs:/logs/entityLogs $Image partitionManager $IP:$SeedPort $PM5ID $NumberOfPartitions &) > logs/partitionManager5.txt 
sleep 1 
echo "Partition Manager $PM5ID up and running at $IP:$PM5Port" 
 
(docker run -p $PM6Port:2551  --rm -e "HOST_IP=$IP" -e "HOST_PORT=$PM6Port" -v $entityLogs:/logs/entityLogs $Image partitionManager $IP:$SeedPort $PM6ID $NumberOfPartitions &) > logs/partitionManager6.txt 
sleep 1 
echo "Partition Manager $PM6ID up and running at $IP:$PM6Port" 
 
(docker run -p $PM7Port:2551  --rm -e "HOST_IP=$IP" -e "HOST_PORT=$PM7Port" -v $entityLogs:/logs/entityLogs $Image partitionManager $IP:$SeedPort $PM7ID $NumberOfPartitions &) > logs/partitionManager7.txt 
sleep 1 
echo "Partition Manager $PM7ID up and running at $IP:$PM7Port" 
 
