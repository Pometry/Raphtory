# Installing into distributed servers

## Information
- Raphtory will run on port 1736 which is the port the client will need to connect on. You will need the security group/firewall pointing to the cluster manager to allow tcp connections to this server on port 1736
- Zookeeper will run on port 2181 and the servers need to talk to each other on a variety of ports. We allow all traffic between this group of machines to facilitate these communications.

## Install Raphtory and dependencies
### Get Raphtory from source code
```
mkdir ~/pometry && cd ~/pometry && git clone https://github.com/Raphtory/Raphtory.git && cd Raphtory
git checkout <branch>
```
### Install sbt 
```
cd /usr/local/bin && sudo apt update && sudo apt install python3-pip unzip make -y && sudo curl -L "https://github.com/sbt/sbt/releases/download/v1.6.2/sbt-1.6.2.zip" -o sbt-1.6.2.zip && sudo unzip sbt-1.6.2.zip && sudo mv sbt sbt-bin && sudo mv sbt-bin/bin/sbt .
```
### Install pip
```
sudo pip install pip -U
```
### Install java
```
wget -O - https://packages.adoptium.net/artifactory/api/gpg/key/public | sudo apt-key add -
echo "deb https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" | sudo tee /etc/apt/sources.list.d/adoptium.list
sudo apt update
sudo apt install temurin-11-jdk -y
```
### Build Raphtory
```
cd ~/pometry/Raphtory && pip install .
# Restart shell here to get the newly installed command line tools
```

## Run zookeeper steps (Cluster Manager machines)
### Install zookeeper
```
cd ~/pometry
curl https://dlcdn.apache.org/zookeeper/zookeeper-3.8.0/apache-zookeeper-3.8.0-bin.tar.gz --output apache-zookeeper-3.8.0-bin.tar.gz
tar xvf apache-zookeeper-3.8.0-bin.tar.gz
cat <<EOF > apache-zookeeper-3.8.0-bin/conf/zoo.cfg
 tickTime=2000
 dataDir=~/pometry/zookeeper-data
 clientPort=2181
EOF
~/pometry/apache-zookeeper-3.8.0-bin/bin/zkServer.sh start
```

## Set Raphtory variables (All machines)
```
export RAPHTORY_CLUSTER_MANAGER_IP_ADDR=<CLUSTER_MANAGER_IP_ADDR>
export RAPHTORY_ZOOKEEPER_ADDRESS=${RAPHTORY_CLUSTER_MANAGER_IP_ADDR}:2181
export RAPHTORY_PARTITIONS_SERVERCOUNT=1
export RAPHTORY_PARTITIONS_COUNTPERSERVER=1
export RAPHTORY_PARTITIONS_CHUNKSIZE=128
export RAPHTORY_CORE_LOG=DEBUG
```

# Run Cluster Manager Service (Cluster Manager machines)
```
raphtory-clustermanager &
```

# Run Query Service (Query machines)
```
raphtory-query &
```

# Run Ingeestion Service (Injestion machines)
```
raphtory-ingestion &
```

# Run Partition Service (Partition machine)
```
raphtory-partition &
```

# Client machine
From your client, invoke python (or Jupiter) and when in python terminal, you can create a context to a remote source
```
from pyraphtory.context import PyRaphtory
ctx = PyRaphtory.remote("<CLUSTER_MANAGER_IP_ADDR>",1736)
graph = ctx.new_graph()
```