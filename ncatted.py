# Open shell process
import subprocess
# Get current working directory
import os
# Split command line arguments
import shlex

def run(att_nm, var_nm, mode, att_type, att_val, hist, inputFile, outputFile=""):
	call = "./ncatted.sh %s %s %s %s %s %s %s %s" % (att_nm, var_nm, mode, att_type, att_val, hist, inputFile, outputFile)
	p = subprocess.Popen(shlex.split(call), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	out, err = p.communicate()
	if err: print(err)