./dockblock.sh
IP="$(./getMyIP.sh)" 
python3 multimachinesetupgen.py 2 2 2 $IP
echo "Once zookeeper has reported that it has connected Press ctrl+p then ctrl+q to disconnect from the container"
docker run -p 2181:2181 -p 2888:2888 -p 3888:3888 -it quay.io/miratepuffin/zookeeper:latest
echo "Disconnected from zookeeper container"
./seedSetup.sh
./machine0Setup.sh
./machine1Setup.sh