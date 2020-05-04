# Building raphtory

##  Requirements
	- sbt (version is fixed within the `build.properties` file)
	- JDK version 12 (be it openjdk or oracle jdk) 
 	- scala (version is fixed within `build.sbt` file)
	- docker (with a login into dockerhub)

## Building
Step 1.
- to build locally, go into mainproject, run `sbt docker:publishLocal`
- to build on dockerhub, go into mainproject, run `sbt:publish`
	
Step 2.
- git clone the raphtory deployment project, https://github.com/miratepuffin/Raphtory-Deployment
- copy the required compose file to the machine you would like to run raphtory
- edit the `.yml` file such that it points to the correct docker hub location
- add the following text into a file called `.env` located in  the same folder as the `.yml` file, change the
parameters of 'UPDATERCLASS' and 'ROUTERCLASS' to the Spout and Router required.

		COMPOSE_PROJECT_NAME=raphtory
		ZOOKEEPER=zooKeeper:2181
		ADD_ONLY_VERTEX=false
		ADD_ONLY_EDGE=false
		JAVA_OPTS=-XX:+UseConcMarkSweepGC -XX:+UseParNewGC -Xms20g -XX:NewRatio=3
		PROMETHEUS=true
		STDOUT_LOG=true
		UPDATERCLASS=com.raphtory.examples.blockchain.spouts.EthereumGethSpout
		ROUTERCLASS=com.raphtory.examples.blockchain.routers.EthereumGethRouter
		RAMP_FLAG=true
		DOCKER=true
		GAB_PROJECT_OUTPUT=/opt/docker/output.csv
		SPOUT_ETHEREUM_IP_ADDRESS=127.0.0.1

- to run this locally, for a single node test edit and then run `docker-compose --file compose/singlenodetest.yml up`
- to run this on a server, copy your `.env` and `.yml` to your server, ensure your server has docker then run `docker-compose --file youryaml.yml up`

## Execute commands via the API
- There is a REST API which allows the user to execute analysis on demand. 
- For example to run the ethereum analysis
    - Run the following as a curl command which will initiate a request to run an analysis
        
            curl -X POST -H "Content-Type: application/json" --data '{"jsonrpc":"2.0", "jobID":"connectedComponentsRangeWindowTest","analyserName":"com.raphtory.examples.blockchain.analysers.TaintTrackExchangeStop","start":9014194,"end":9907863,"jump":10,"args":["0xa09871aeadf4994ca12f5c0b6056bbd1d343c029","9007863"]}' 127.0.0.1:8081/RangeAnalysisRequest
    
    - Next run the following to obtain the results
        
            curl 127.0.0.1:8081/AnalysisResults\?jobID=connectedComponentsRangeWindowTest

## Failed build due to URI ERROR
As of 4th May 2020, JDK13 causes build errors when used with SBT. 
This is a known issue which causes a '/packages cannot be represented as URI' error, 
that subsequently fails the build. See this issue here https://github.com/sbt/sbt/issues/5093
If you use OSX theres another problem, homebrew by default uses JDK13, which causes sbt to use 
this JDK despite whichever version you currently active. 

To fix this
- Install https://www.jenv.be/ 
- Add your OpenJDK 12 to the jenv environment
- Switch the environment with the jenv command to use the OpenJDK10
- then run the build

#### For example on OSX my commands were
- Install jenv via brew and export the paths to your environment profile or bashrc
    - `brew install jenv`
- Add the java environment 
    - You can install openjdk12 on MacOSX by doing this
        - `brew tap AdoptOpenJDK/openjdk`
        - `brew cask install adoptopenjdk12`
    - `jenv add  /Library/Java/JavaVirtualMachines/adoptopenjdk-10.jdk/Contents/Home/`
- Go into the main project folder
    - `cd raphtory/mainproject`
- Change the local java environment (per folder and everything in that folder)
    - `jenv local 12.0.2`
- Now sbt will run without any errors
    - `sbt build:publishLocal`
 
