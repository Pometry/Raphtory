import sys


def metadata(file):
	#scripts for getting running IP and killing off existing containers
	file.write("#!/usr/bin/env bash \n \n")
	file.write("IP=\"$(./getMyIP.sh)\" \n \n")
	file.write("(docker ps -aq --no-trunc | xargs docker rm) >/dev/null\n \n" )
	file.write("ZooKeeper=\""+zookeeperLoc+"\" \n \n")
	file.write("LAMName=\"testLam\" \n \n")
	file.write("Image=\"quay.io/miratepuffin/cluster\" #if you want to use prebuilt one on my quay.io \n \n")

	#write out number of partitions to be parsed to routers
	file.write("NumberOfPartitions="+str(NumberOfPartitions)+"\n \n")
	file.write("NumberOfUpdates="+str(NumberOfUpdates)+"\n \n")
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
	file.write("BenchmarkPort=9104 \n \n")
	file.write("LiveAnalysisPort=9105 \n \n")


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
		files[(i%NumberOfMachines)].write("(docker run --name=\"partition"+str(i)+"\"  -p $PM"+str(i)+"Port:$PM"+str(i)+"Port  --rm -e \"BIND_PORT=$PM"+str(i)+"Port\" -e \"HOST_IP=$IP\" -e \"HOST_PORT=$PM"+str(i)+"Port\" -v $entityLogs:/logs/entityLogs $Image partitionManager $PM"+str(i)+"ID $NumberOfPartitions $ZooKeeper &) > logs/"+files[(i%NumberOfMachines)].name[:len(files[(i%NumberOfMachines)].name)-3]+"/partitionManager"+str(i)+".txt \n")
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
		files[(i%NumberOfMachines)].write("(docker run --name=\"router"+str(i)+"\" -p $Router"+str(i)+"Port:$Router"+str(i)+"Port  --rm -e \"BIND_PORT=$Router"+str(i)+"Port\" -e \"HOST_IP=$IP\" -e \"HOST_PORT=$Router"+str(i)+"Port\" $Image router $NumberOfPartitions $ZooKeeper &) > logs/"+files[(i%NumberOfMachines)].name[:len(files[(i%NumberOfMachines)].name)-3]+"/router"+str(i)+".txt \n")
		files[(i%NumberOfMachines)].write("sleep 1 \n")
		files[(i%NumberOfMachines)].write("echo \"Router "+str(i)+" up and running at $IP:$Router"+str(i)+"Port\" \n \n")

def updatePorts(files):
	#For each updater, generate a port
	for i in range(0,NumberOfUpdaters):
		id = "Update"+str(i)+"Port="
		port = 9400+i
		files[(i%NumberOfMachines)].write(id+str(port)+"\n")

def updateRun(files):
	#Updaters
	for i in range(0,NumberOfUpdaters):
		files[(i%NumberOfMachines)].write("(docker run --name=\"updater"+str(i)+"\" -p $Update"+str(i)+"Port:$Update"+str(i)+"Port  --rm -e \"BIND_PORT=$Update"+str(i)+"Port\" -e \"HOST_IP=$IP\" -e \"HOST_PORT=$Update"+str(i)+"Port\" $Image updateGen $NumberOfPartitions $NumberOfUpdates $ZooKeeper &) > logs/"+files[(i%NumberOfMachines)].name[:len(files[(i%NumberOfMachines)].name)-3]+"/updateGenerator"+str(i)+".txt \n")
		files[(i%NumberOfMachines)].write("sleep 1 \n")
		files[(i%NumberOfMachines)].write("echo \"Update Generator "+str(i)+" up and running at $IP:$Update"+str(i)+"Port\" \n \n")		

def dockerblock(file):
	#seed node
	file.write("(docker run --name=\"seednode\" -p $SeedPort:$SeedPort --rm -e \"BIND_PORT=$SeedPort\" -e \"HOST_IP=$IP\" -e \"HOST_PORT=$SeedPort\" $Image seed $IP:$SeedPort $ZooKeeper &) > logs/"+file.name[:len(file.name)-3]+"/seed.txt \n")
	file.write("sleep 5 \n")
	file.write("echo \"Seed node up and running at $IP:$SeedPort\" \n \n")
	
	#Rest API Node
	file.write("(docker run --name=\"rest\"  -p $RestPort:$RestPort -p 8080:8080 --rm -e \"BIND_PORT=$RestPort\" -e \"HOST_IP=$IP\" -e \"HOST_PORT=$RestPort\" $Image rest $ZooKeeper &) > logs/"+file.name[:len(file.name)-3]+"/rest.txt \n")
	file.write("sleep 1 \n")
	file.write("echo \"REST API node up and running at $IP:$RestPort\" \n \n")

	#benchmark node
	file.write("(docker run --name=\"benchmarker\"  -p $BenchmarkPort:$BenchmarkPort  --rm -e \"BIND_PORT=$BenchmarkPort\" -e \"HOST_IP=$IP\" -e \"HOST_PORT=$BenchmarkPort\" $Image benchmark $NumberOfPartitions $ZooKeeper &) > logs/"+file.name[:len(file.name)-3]+"/benchmark.txt \n")
	file.write("sleep 1 \n")
	file.write("echo \"Benchmarker up and running at $IP:$BenchmarkPort\" \n \n")
	
	#live analysis manager
	#file.write("(docker run --name=\"lam\"  -p $LiveAnalysisPort:$LiveAnalysisPort  --rm -e \"BIND_PORT=$LiveAnalysisPort\" -e \"HOST_IP=$IP\" -e \"HOST_PORT=$LiveAnalysisPort\" $Image LiveAnalysisManager $NumberOfPartitions $ZooKeeper $LAMName &) > logs/"+file.name[:len(file.name)-3]+"/LiveAnalysisManager.txt \n")
	#file.write("sleep 1 \n")
	#file.write("echo \"Live Analyser running at $IP:$LiveAnalysisPort\" \n \n")

def complete(file):
	file.write("ClusterUpPort=9106 \n \n")
	file.write("(docker run --name=\"clusterUp\"  -p $ClusterUpPort:$ClusterUpPort  --rm -e \"BIND_PORT=$ClusterUpPort\" -e \"HOST_IP=$IP\" -e \"HOST_PORT=$ClusterUpPort\" $Image ClusterUp $NumberOfPartitions $ZooKeeper &) > logs/"+file.name[:len(file.name)-3]+"/ClusterUp.txt \n")
	file.write("sleep 15 \n")
	file.write("echo \"CLUSTER UP\"\n \n")


#get all required arguments
NumberOfPartitions=int(sys.argv[1])
NumberOfRouters=int(sys.argv[2])
NumberOfUpdaters=int(sys.argv[3])
NumberOfMachines=int(sys.argv[4])
NumberOfUpdates=int(sys.argv[5])
zookeeperLoc = sys.argv[6]

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

#write ports for meta containers to all files
seedports(seedFile)

#split desired number of parition managers amongst set number of servers
partitionManagerPorts(machinefiles)
partitionManagerIDs(machinefiles)
partitionManagerRun(machinefiles)

#split routers amongst servers
routerPorts(machinefiles)
routerRun(machinefiles)

#split updates amongst servers
updatePorts(machinefiles)
updateRun(machinefiles)

#write run command for all meta containers to 
dockerblock(seedFile)

complete(machinefiles[len(machinefiles)-1])


#close files
seedFile.close()
for f in machinefiles:
	f.close()











