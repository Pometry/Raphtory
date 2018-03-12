./dockblock.sh
IP="$(./getMyIP.sh):2181" 
python3 multimachinesetupgen.py 1 1 1 1 $IP
echo "Once zookeeper has reported that it has connected Press ctrl+p then ctrl+q to disconnect from the container"
./runzoo.sh
echo "Disconnected from zookeeper container"
./seedSetup.sh
./machine0Setup.sh
#./machine1Setup.sh