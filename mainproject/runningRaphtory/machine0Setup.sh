#!/usr/bin/env bash 
 
IP="$(./getMyIP.sh)" 
 
(docker ps -aq --no-trunc | xargs docker rm) >/dev/null
 
ZooKeeper="161.23.245.235:2181" 
 
LAMName="testLam" 
 
Image="quay.io/miratepuffin/cluster" #if you want to use prebuilt one on my quay.io 
 
NumberOfPartitions=1
 
if [ ! -d logs ]; then mkdir logs; fi 
rm -r logs/machine0Setup
if [ ! -d logs/machine0Setup ]; then mkdir logs/machine0Setup; fi 
chmod 777 logs 
chmod 777 logs/machine0Setup
logsdir=$(pwd)"/logs/machine0Setup/" 
 
PM0Port=9200
PM0ID=0
(docker run --name="partition0" -p $PM0Port:$PM0Port --rm -e "BIND_PORT=$PM0Port" -e "HOST_IP=$IP" -e "HOST_PORT=$PM0Port" -v $logsdir:/logs $Image partitionManager $PM0ID $NumberOfPartitions $ZooKeeper &) > logs/machine0Setup/partitionManager0.txt 
sleep 2 
echo "Partition Manager $PM0ID up and running at $IP:$PM0Port" 
 
Router0Port=9300
(docker run --name="router0" -p $Router0Port:$Router0Port  --rm -e "BIND_PORT=$Router0Port" -e "HOST_IP=$IP" -e "HOST_PORT=$Router0Port" $Image router $NumberOfPartitions $ZooKeeper &) > logs/machine0Setup/router0.txt 
sleep 1 
echo "Router 0 up and running at $IP:$Router0Port" 
 
Update0Port=9400
(docker run --name="updater0" -p $Update0Port:$Update0Port  --rm -e "BIND_PORT=$Update0Port" -e "HOST_IP=$IP" -e "HOST_PORT=$Update0Port" $Image updateGen $NumberOfPartitions $ZooKeeper &) > logs/machine0Setup/updateGenerator0.txt 
sleep 1 
echo "Update Generator 0 up and running at $IP:$Update0Port" 
 
ClusterUpPort=9106 
 
(docker run --name="clusterUp"  -p $ClusterUpPort:$ClusterUpPort  --rm -e "BIND_PORT=$ClusterUpPort" -e "HOST_IP=$IP" -e "HOST_PORT=$ClusterUpPort" $Image ClusterUp $NumberOfPartitions $ZooKeeper &) > logs/machine0Setup/ClusterUp.txt 
sleep 15 
echo "CLUSTER UP"
 
