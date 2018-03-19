./dockblock.sh
IP="$(./getMyIP.sh):2181" 
python3 multimachinesetupgen.py 1 1 1 1 $IP
./runzoo.sh
./seedSetup.sh
./machine0Setup.sh
#./machine1Setup.sh