#!/bin/bash

# Delete all history from netCDF file

input_file=$1

ncatted -a history,global,d,, -h $input_file

ncdump -h $input_file