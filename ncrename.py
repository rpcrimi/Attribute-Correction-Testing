# Open shell process
import subprocess
# Get current working directory
import os
# Split command line arguments
import shlex

def run(oldName, newName, inputFile, outputFile=""):
	call = "./ncrename.sh %s %s %s %s" % (oldName, newName, inputFile, outputFile)
	p = subprocess.Popen(shlex.split(call), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	out, err = p.communicate()
	if err: print err