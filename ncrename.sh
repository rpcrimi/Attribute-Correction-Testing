#!/bin/bash

original=$1
new=$2
inputFile=$3
outputFile=$4

ncrename -v $original,$new -h $inputFile -O $outputFile