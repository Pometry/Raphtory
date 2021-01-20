#!/bin/sh
targetScript=./analysis-utils.sh

#1900
#startTime=116877
#1940
startTime=124182
#2020
endTime=153402
#windowSize=7300
#365*20
#windowSize=3650
#365*10
windowSize=365
algoPackage=com.raphtory.algorithms.
#algoNames=( DegreeBasic PageRank WeightedPageRank ConnectedComponents DegreeRanking DegreeDistribution TriangleCount)
algoNames=( DegreeBasic WeightedPageRank ConnectedComponents TriangleCount)

#multiples of 365
#default 50 25 10 5 1
windows="18250,9125,3650,1825,365"

#analysisName=_1920_2020_aust
analysisName=_1940_2020_full_external

for i in "${algoNames[@]}"
do
#	echo "$targetScript $i$analysisName $algoPackage$i $startTime $endTime $windowSize $windows"
	echo `$targetScript $i$analysisName $algoPackage$i $startTime $endTime $windowSize $windows`
	echo ""
	echo 'sleep 1'
done

#./analysisutils.sh $algoName$analysisName com.raphtory.algorithms.DegreeBasic $startTime $endTime $windowSize $windows
#./analysisutils.sh PageRank_1920_2020_aust com.raphtory.algorithms.PageRank 116877 153402 7300 "18250,9125,3650,1825,365"
#./analysisutils.sh ConnectedComponents_1920_2020_aust com.raphtory.algorithms.ConnectedComponents 116877 153402 7300 "18250,9125,3650,1825,365"
#./analysisutils.sh DegreeRanking_1920_2020_aust com.raphtory.algorithms.DegreeRanking 116877 153402 7300 "18250,9125,3650,1825,365"
