import sys

#get all required arguments
NumberOfPartitions=int(sys.argv[1])
NumberOfRouters=int(sys.argv[2])
#LocalOrRemote = bool(sys.argv[3])

#open file for writing
file = open("autoSetup.sh","w")

#scripts for getting running IP and killing off existing containers
file.write("#!/usr/bin/env bash \n \n")
file.write("IP=\"$(./getMyIP.sh)\" \n \n")
file.write("./dockblock.sh \n \n")

#image name (local or quay)
#if(LocalOrRemote):
#	file.write("Image=\"dockerexp/cluster\" #local if you build your own from the source files \n \n")
#else:
file.write("Image=\"quay.io/miratepuffin/cluster\" #if you want to use prebuilt one on my quay.io \n \n")

#create/clear log folder
file.write("rm -r logs \n")
file.write("if [ ! -d logs ]; then mkdir logs; fi \n")
file.write("if [ -d logs/entityLogs/ ]; then rm -r logs/entityLogs/; fi \n")
file.write("mkdir logs/entityLogs/ \n")
file.write("entityLogs=$(pwd)\"/logs/entityLogs\" \n \n")

file.write ("chmod 777 logs \n")
file.write ("chmod 777 logs/entityLogs\n \n")
#write out number of partitions to be parsed to routers
file.write("NumberOfPartitions="+str(NumberOfPartitions)+"\n \n")

#PORT BLOCK
file.write("SeedPort=9101 \n")
file.write("RestPort=9102 \n")
file.write("UpdatePort=9103 \n")
file.write("BenchmarkPort=9104 \n \n")
file.write("LiveAnalysisPort=9105 \n \n")

file.write("JVM=\"-Dcom.sun.management.jmxremote.rmi.port=9090 -Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.port=9090  -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.local.only=false -Djava.rmi.server.hostname=$IP\" \n")

#For each partition manager, generate a port
for i in range(0,NumberOfPartitions):
	id = "PM"+str(i)+"Port="
	port = 9200+i
	file.write(id+str(port)+"\n")
file.write("\n")

#For each partition manager, generate an ID
for i in range(0,NumberOfPartitions):
	id = "PM"+str(i)+"ID="+str(i)
	file.write(id+"\n")
file.write("\n")

#For each router, generate a port
for i in range(0,NumberOfRouters):
	id = "Router"+str(i)+"Port="
	port = 9300+i
	file.write(id+str(port)+"\n")
file.write("\n")

#DOCKER BLOCK

#Spacing 
file.write("\n \n")

#seed node
file.write("(docker run -p $SeedPort:2551 --rm -e \"HOST_IP=$IP\" -e \"HOST_PORT=$SeedPort\" $Image seed &) > logs/seed.txt \n")
file.write("sleep 1 \n")
file.write("echo \"Seed node up and running at $IP:$SeedPort\" \n \n")

#Rest API Node
file.write("(docker run -p $RestPort:2551 -p 8080:8080 --rm -e \"HOST_IP=$IP\" -e \"HOST_PORT=$RestPort\" $Image rest $IP:$SeedPort &) > logs/rest.txt \n")
file.write("sleep 1 \n")
file.write("echo \"REST API node up and running at $IP:$RestPort\" \n \n")

#Update Node
file.write("(docker run -p $UpdatePort:2551  --rm -e \"HOST_IP=$IP\" -e \"HOST_PORT=$UpdatePort\" $Image updateGen $IP:$SeedPort $NumberOfPartitions &) > logs/updateGenerator.txt \n")
file.write("sleep 1 \n")
file.write("echo \"Update Generator up and running at $IP:$UpdatePort\" \n \n")

#benchmark node
file.write("(docker run -p $BenchmarkPort:2551  --rm -e \"HOST_IP=$IP\" -e \"HOST_PORT=$BenchmarkPort\" $Image benchmark $IP:$SeedPort $NumberOfPartitions &) > logs/benchmark.txt \n")
file.write("sleep 1 \n")
file.write("echo \"Benchmarker up and running at $IP:$BenchmarkPort\" \n \n")

#Spacing 
file.write("\n \n")

#Routers
for i in range(0,NumberOfRouters):
	file.write("(docker run -p $Router"+str(i)+"Port:2551  --rm -e \"HOST_IP=$IP\" -e \"HOST_PORT=$Router"+str(i)+"Port\" $Image router $IP:$SeedPort $NumberOfPartitions &) > logs/router"+str(i)+".txt \n")
	file.write("sleep 1 \n")
	file.write("echo \"Router "+str(i)+" up and running at $IP:$Router"+str(i)+"Port\" \n \n")

#Spacing 
file.write("\n \n")

#partition nodes
for i in range(0,NumberOfPartitions):
	file.write("(docker run -p $PM"+str(i)+"Port:2551  --rm -e \"HOST_IP=$IP\" -e \"HOST_PORT=$PM"+str(i)+"Port\" -v $entityLogs:/logs/entityLogs $Image partitionManager $IP:$SeedPort $PM"+str(i)+"ID $NumberOfPartitions &) > logs/partitionManager"+str(i)+".txt \n")
	file.write("sleep 2 \n")
	file.write("echo \"Partition Manager $PM"+str(i)+"ID up and running at $IP:$PM"+str(i)+"Port\" \n \n")
	
file.write("(docker run -p $LiveAnalysisPort:2551  --rm -e \"HOST_IP=$IP\" -e \"HOST_PORT=$LiveAnalysisPort\" $Image LiveAnalysisManager $IP:$SeedPort $NumberOfPartitions &) > logs/LiveAnalysisManager.txt \n")
file.write("sleep 1 \n")
file.write("echo \"Live Analyser running at $IP:$LiveAnalysisPort\" \n \n")


file.close()












