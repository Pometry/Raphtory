#!/bin/sh
#curl -X POST -H "Content-Type: application/json" --data '{"jsonrpc":"2.0", "jobID":"degreebasic_1920_2020_aust","analyserName":"com.raphtory.algorithms.DegreeBasic","start":116877,"end":153402,"jump":7300,"windowType":"batched","windowSet":[18250,9125,3650,1825,365]}' 127.0.0.1:8081/RangeAnalysisRequest


setup_range_function() {
    rm output
    touch output
#    echo "Start of function..."
    printf "curl -X POST -H \"Content-Type: application/json\" --data '{\"jsonrpc\":\"2.0\", \"jobID\":\"%s\" " $1 >> output
    printf ",\"analyserName\":\"%s\" " $2 >> output
    printf ",\"start\":%s,\"end\":%s,\"jump\":%s,\"windowType\":\"batched\" " $3 $4 $5 >> output
#    18250,9125,3650,1825,365
    printf ",\"windowSet\":[%s]}' " $6 >> output
    printf "127.0.0.1:8081/RangeAnalysisRequest" >> output
    echo "" >> output
#    echo "end of function..."
}


#if [ $1 = "Range" ]
#then
#  setup_range_function $1 $2 $3 $4 $5 $6
#fi
#echo "Start of script..."
setup_range_function $1 $2 $3 $4 $5 $6
cat output
echo ""
#echo "end of script..."
#degreebasic_1920_2020_aust com.raphtory.algorithms.DegreeBasic 116877 153402 7300 "18250,9125,3650,1825,365"
#./raphtoryLocal Range ConnectedComponents 1470783600000 1476114624000 86400000 false 86400000
