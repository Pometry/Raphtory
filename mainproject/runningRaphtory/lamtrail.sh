#!/usr/bin/env bash 
 
IP="$(./getMyIP.sh)" 
 
ZooKeeper="161.23.245.65" 
 
LAMName="testLam2" 
 
Image="quay.io/miratepuffin/cluster" #if you want to use prebuilt one on my quay.io 
 
NumberOfPartitions=2
 
LiveAnalysisPort=9800
 

 
(docker run -p $LiveAnalysisPort:$LiveAnalysisPort  --rm -e "BIND_PORT=$LiveAnalysisPort" -e "HOST_IP=$IP" -e "HOST_PORT=$LiveAnalysisPort" $Image LiveAnalysisManager $NumberOfPartitions $ZooKeeper $LAMName &) > logs/seedSetup/LiveAnalysisManager2.txt 
sleep 1 
echo "Live Analyser running at $IP:$LiveAnalysisPort" 