#!/bin/bash
#$ -cwd           # Set the working directory for the job to the current directory
#$ -pe smp 1      # Request 1 core
#$ -l h_rt=1:0:0 # Request 1 hour runtime
#$ -l h_vmem=1G   # Request 8GB RAM
module load singularity
getMyIP() {
    local _ip _myip _line _nl=$'\n'
    while IFS=$': \t' read -a _line ;do
        [ -z "${_line%inet}" ] &&
           _ip=${_line[${#_line[1]}>4?1:2]} &&
           [ "${_ip#127.0.0.1}" ] && _myip=$_ip
      done< <(LANG=C /sbin/ifconfig)
    printf ${1+-v} $1 "%s${_nl:0:$[${#1}>0?0:1]}" $_myip
}
getMyIP
IP=$getMyIP
echo $IP
ZooKeeper="10.14.100.24:2181" 
NumberOfPartitions=1


if [ -d logs ]; then rm -r logs; fi 
mkdir logs
mkdir logs/entityLogs
chmod 777 logs 
chmod 777 logs/entityLogs
 
SeedPort=9101 
RestPort=9102 
UpdatePort=9103 
BenchmarkPort=9104  
LiveAnalysisPort=9105 
ClusterUpPort=9106 
PM0Port=9200
PM0ID=0
Router0Port=9300

SINGULARITYENV_HOST_PORT=$SeedPort SINGULARITYENV_HOST_IP=$IP SINGULARITYENV_BIND_PORT=$SeedPort singularity exec cluster.img /opt/docker/bin/cluster seed $IP:$SeedPort $ZooKeeper
#sleep 2
#(SINGULARITYENV_HOST_PORT=$RestPort SINGULARITYENV_HOST_IP=$IP SINGULARITYENV_BIND_PORT=$RestPort singularity exec cluster.img /opt/docker/bin/cluster rest $ZooKeeper &) >logs/rest.txt
#sleep 2
#(SINGULARITYENV_HOST_PORT=$UpdatePort SINGULARITYENV_HOST_IP=$IP SINGULARITYENV_BIND_PORT=$UpdatePort singularity exec cluster.img /opt/docker/bin/cluster updateGen $NumberOfPartitions $ZooKeeper &) >logs/update.txt
#sleep 2
#(SINGULARITYENV_HOST_PORT=$BenchmarkPort SINGULARITYENV_HOST_IP=$IP SINGULARITYENV_BIND_PORT=$BenchmarkPort singularity exec cluster.img /opt/docker/bin/cluster benchmark $NumberOfPartitions $ZooKeeper &) > logs/benchmark.txt
#sleep 2
#(SINGULARITYENV_HOST_PORT=$LiveAnalysisPort SINGULARITYENV_HOST_IP=$IP SINGULARITYENV_BIND_PORT=$LiveAnalysisPort singularity exec cluster.img /opt/docker/bin/cluster LiveAnalysisManager $NumberOfPartitions $ZooKeeper &) > logs/Live.txt
#sleep 2
#(SINGULARITYENV_HOST_PORT=$PM0Port SINGULARITYENV_HOST_IP=$IP SINGULARITYENV_BIND_PORT=$PM0Port singularity exec cluster.img /opt/docker/bin/cluster partitionManager $PM0ID $NumberOfPartitions $ZooKeeper &) > logs/pm1.txt
#sleep 2
#(SINGULARITYENV_HOST_PORT=$Router0Port SINGULARITYENV_HOST_IP=$IP SINGULARITYENV_BIND_PORT=$Router0Port singularity exec cluster.img /opt/docker/bin/cluster router $NumberOfPartitions $ZooKeeper &) > logs/r1.txt
#sleep 2
#(SINGULARITYENV_HOST_PORT=$ClusterUpPort SINGULARITYENV_HOST_IP=$IP SINGULARITYENV_BIND_PORT=$ClusterUpPort singularity exec cluster.img /opt/docker/bin/cluster ClusterUp $NumberOfPartitions $ZooKeeper &) > logs/clusterUp.txt

