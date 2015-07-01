#!/bin/bash

original=$1
new=$2
inputFile=$3
outputFile=$4

ncrename -v $original,$new -d .$original,$new -h $inputFile -o $outputFile