import sys


def metadata(file):
	#scripts for getting running IP and killing off existing containers
	file.write("#!/usr/bin/env bash \n \n")
	file.write("IP=\"$(./getMyIP.sh)\" \n \n")
	file.write("ZooKeeper=\""+zookeeperLoc+"\" \n \n")
	#file.write("./dockblock.sh \n \n")
	file.write("Image=\"quay.io/miratepuffin/cluster\" #if you want to use prebuilt one on my quay.io \n \n")

	#write out number of partitions to be parsed to routers
	file.write("NumberOfPartitions="+str(NumberOfPartitions)+"\n \n")
	#for investigation of internal JVM
	file.write("JVM=\"-Dcom.sun.management.jmxremote.rmi.port=9090 -Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.port=9090  -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.local.only=false -Djava.rmi.server.hostname=$IP\" \n")


def logs(file):
	#create/clear log folder
	file.write("if [ ! -d logs ]; then mkdir logs; fi \n")
	file.write("rm -r logs/"+file.name[:len(file.name)-3]+"\n")
	file.write("if [ ! -d logs/"+file.name[:len(file.name)-3]+" ]; then mkdir logs/"+file.name[:len(file.name)-3]+"; fi \n")
	file.write("if [ ! -d logs/"+file.name[:len(file.name)-3]+"/entityLogs/ ]; then mkdir logs/"+file.name[:len(file.name)-3]+"/entityLogs/; fi \n")
	file.write("entityLogs=$(pwd)\"/logs/"+file.name[:len(file.name)-3]+"/entityLogs\" \n \n")
	file.write ("chmod 777 logs \n")
	file.write ("chmod 777 logs/"+file.name[:len(file.name)-3]+"\n")
	file.write ("chmod 777 logs/"+file.name[:len(file.name)-3]+"/entityLogs\n \n")


def seedports(file):
	#PORT BLOCK
	file.write("SeedPort=9101 \n")
	file.write("RestPort=9102 \n")
	file.write("UpdatePort=9103 \n")
	file.write("BenchmarkPort=9104 \n \n")
	file.write("LiveAnalysisPort=9105 \n \n")
	file.write("ClusterUpPort=9106 \n \n")

def partitionManagerPorts(files):
	#For each partition manager, generate a port
	for i in range(0,NumberOfPartitions):
		id = "PM"+str(i)+"Port="
		port = 9200+i
		files[(i%NumberOfMachines)].write(id+str(port)+"\n")


def partitionManagerIDs(files):
	#For each partition manager, generate an ID
	for i in range(0,NumberOfPartitions):
		id = "PM"+str(i)+"ID="+str(i)
		files[(i%NumberOfMachines)].write(id+"\n")

def partitionManagerRun(files):
	#partition nodes
	for i in range(0,NumberOfPartitions):
		files[(i%NumberOfMachines)].write("(docker run -p $PM"+str(i)+"Port:2551  --rm -e \"HOST_IP=$IP\" -e \"HOST_PORT=$PM"+str(i)+"Port\" -v $entityLogs:/logs/entityLogs $Image partitionManager $PM"+str(i)+"ID $NumberOfPartitions $ZooKeeper &) > logs/"+files[(i%NumberOfMachines)].name[:len(files[(i%NumberOfMachines)].name)-3]+"/partitionManager"+str(i)+".txt \n")
		files[(i%NumberOfMachines)].write("sleep 2 \n")
		files[(i%NumberOfMachines)].write("echo \"Partition Manager $PM"+str(i)+"ID up and running at $IP:$PM"+str(i)+"Port\" \n \n")

def routerPorts(files):
	#For each router, generate a port
	for i in range(0,NumberOfRouters):
		id = "Router"+str(i)+"Port="
		port = 9300+i
		files[(i%NumberOfMachines)].write(id+str(port)+"\n")

def routerRun(files):
	#Routers
	for i in range(0,NumberOfRouters):
		files[(i%NumberOfMachines)].write("(docker run -p $Router"+str(i)+"Port:2551  --rm -e \"HOST_IP=$IP\" -e \"HOST_PORT=$Router"+str(i)+"Port\" $Image router $NumberOfPartitions $ZooKeeper &) > logs/"+files[(i%NumberOfMachines)].name[:len(files[(i%NumberOfMachines)].name)-3]+"/router"+str(i)+".txt \n")
		files[(i%NumberOfMachines)].write("sleep 1 \n")
		files[(i%NumberOfMachines)].write("echo \"Router "+str(i)+" up and running at $IP:$Router"+str(i)+"Port\" \n \n")

def dockerblock(file):
	#seed node
	file.write("(docker run -p $SeedPort:2551 --rm -e \"HOST_IP=$IP\" -e \"HOST_PORT=$SeedPort\" $Image seed $IP:$SeedPort $ZooKeeper &) > logs/"+file.name[:len(file.name)-3]+"/seed.txt \n")
	file.write("sleep 5 \n")
	file.write("echo \"Seed node up and running at $IP:$SeedPort\" \n \n")
	
	#Rest API Node
	file.write("(docker run -p $RestPort:2551 -p 8080:8080 --rm -e \"HOST_IP=$IP\" -e \"HOST_PORT=$RestPort\" $Image rest $ZooKeeper &) > logs/"+file.name[:len(file.name)-3]+"/rest.txt \n")
	file.write("sleep 1 \n")
	file.write("echo \"REST API node up and running at $IP:$RestPort\" \n \n")

	#Update Node
	file.write("(docker run -p $UpdatePort:2551  --rm -e \"HOST_IP=$IP\" -e \"HOST_PORT=$UpdatePort\" $Image updateGen $NumberOfPartitions $ZooKeeper &) > logs/"+file.name[:len(file.name)-3]+"/updateGenerator.txt \n")
	file.write("sleep 1 \n")
	file.write("echo \"Update Generator up and running at $IP:$UpdatePort\" \n \n")

	#benchmark node
	file.write("(docker run -p $BenchmarkPort:2551  --rm -e \"HOST_IP=$IP\" -e \"HOST_PORT=$BenchmarkPort\" $Image benchmark $NumberOfPartitions $ZooKeeper &) > logs/"+file.name[:len(file.name)-3]+"/benchmark.txt \n")
	file.write("sleep 1 \n")
	file.write("echo \"Benchmarker up and running at $IP:$BenchmarkPort\" \n \n")
	
	#live analysis manager
	file.write("(docker run -p $LiveAnalysisPort:2551  --rm -e \"HOST_IP=$IP\" -e \"HOST_PORT=$LiveAnalysisPort\" $Image LiveAnalysisManager $NumberOfPartitions $ZooKeeper &) > logs/"+file.name[:len(file.name)-3]+"/LiveAnalysisManager.txt \n")
	file.write("sleep 1 \n")
	file.write("echo \"Live Analyser running at $IP:$LiveAnalysisPort\" \n \n")

def complete(file):
	file.write("(docker run -p $ClusterUpPort:2551  --rm -e \"HOST_IP=$IP\" -e \"HOST_PORT=$ClusterUpPort\" $Image ClusterUp $NumberOfPartitions $ZooKeeper &) > logs/"+file.name[:len(file.name)-3]+"/ClusterUp.txt \n")
	file.write("sleep 1 \n")
	file.write("echo \"CLUSTER UP\"\n \n")


#get all required arguments
NumberOfPartitions=int(sys.argv[1])
NumberOfRouters=int(sys.argv[2])
NumberOfMachines=int(sys.argv[3])
#zookeeperLoc = sys.argv[4]
zookeeperLoc = "161.23.245.93:2181,161.23.245.93:2182,161.23.245.93:2183"


#open files for writing
seedFile = open("seedSetup.sh","w")
machinefiles = [open("machine{}Setup.sh".format(x),"w") for x in range(0,NumberOfMachines)]

#write meta data to each file 
metadata(seedFile)
for f in machinefiles:
	metadata(f)

#write logs to each file
logs(seedFile)
for f in machinefiles:
	logs(f)

#write ports for meta containers to all files (only really needed in seed, but might as well)
seedports(seedFile)
for f in machinefiles:
	seedports(f)

#split desired number of parition managers amongst set number of servers
partitionManagerPorts(machinefiles)
partitionManagerIDs(machinefiles)
partitionManagerRun(machinefiles)

#split routers amongst servers
routerPorts(machinefiles)
routerRun(machinefiles)

#write run command for all meta containers to 
dockerblock(seedFile)

complete(machinefiles[len(machinefiles)-1])


#close files
seedFile.close()
for f in machinefiles:
	f.close()











