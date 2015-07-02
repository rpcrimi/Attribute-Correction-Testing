# Open shell process
import subprocess
# Get current working directory
import os
# Split command line arguments
import shlex

def run(oldName, newName, inputFile, hist, outputFile=""):
	call = "./ncrename.sh %s %s %s %s %s" % (oldName, newName, hist, inputFile, outputFile)
	p = subprocess.Popen(shlex.split(call))
	out, err = p.communicate()
	if err: print err