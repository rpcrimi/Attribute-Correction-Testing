#!/bin/bash

original=$1
new=$2
inputFile=$3
outputFile=$4
hist=$5

# Keep History
if [ "$hist" = -h ]
then
	ncrename -v $original,$new -d .$original,$new -h $inputFile -O $outputFile
# Change History
else
	ncrename -v $original,$new -d .$original,$new $inputFile -O $outputFile
fi
