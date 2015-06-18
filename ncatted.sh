#!/bin/bash

att_dsc=$1,$2,$3,$4,$5
hist=$6
input_file=$7

# Keep History
if [ $hist = -h ]
then
	ncatted -a $att_dsc -h $input_file
# Change History
else
	ncatted -a $att_dsc $input_file
fi


# Dump the header of the updated file
ncdump -h $input_file