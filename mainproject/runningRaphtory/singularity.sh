#!/usr/bin/env bash 
 
IP="$(./getMyIP.sh)" 
 
ZooKeeper="$IP:2181" 
 

 
SeedPort=9101 
RestPort=9102 
UpdatePort=9103
SINGULARITYENV_HOST_PORT=$SeedPort SINGULARITYENV_HOST_IP=$IP SINGULARITYENV_BIND_PORT=$SeedPort singularity exec cluster.simg /opt/docker/bin/cluster seed $IP:$SeedPort $ZooKeeper
SINGULARITYENV_HOST_PORT=$RestPort SINGULARITYENV_HOST_IP=$IP SINGULARITYENV_BIND_PORT=$RestPort singularity exec cluster.simg /opt/docker/bin/cluster rest $ZooKeeper
SINGULARITYENV_HOST_PORT=$UpdatePort SINGULARITYENV_HOST_IP=$IP SINGULARITYENV_BIND_PORT=$UpdatePort 

(docker run -p $UpdatePort:$UpdatePort  --rm -e "BIND_PORT=$UpdatePort" -e "HOST_IP=$IP" -e "HOST_PORT=$UpdatePort" $Image updateGen $NumberOfPartitions $ZooKeeper &) > logs/seedSetup/updateGenerator.txt 


chmod 777 logs 
chmod 777 logs/seedSetup
chmod 777 logs/seedSetup/entityLogs

( &) > logs/seedSetup/seed.txt 

 
(docker run -p $RestPort:$RestPort -p 8080:8080 --rm -e "BIND_PORT=$RestPort" -e "HOST_IP=$IP" -e "HOST_PORT=$RestPort" $Image rest $ZooKeeper &) > logs/seedSetup/rest.txt 
