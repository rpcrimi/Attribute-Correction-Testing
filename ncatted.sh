#!/bin/bash

att_dsc=$1,$2,$3,$4,$5
input_file=$6
output_file=$7
hist=$8

# Keep History
if [ "$hist" = -h ]
then
	ncatted -a "$att_dsc" -h $input_file -O $output_file
# Change History
else
	ncatted -a "$att_dsc" $input_file -O $output_file
fi