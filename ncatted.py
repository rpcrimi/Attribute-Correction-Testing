# Open shell process
import subprocess
# Split command line arguments
import shlex

def run(att_nm, var_nm, mode, att_type, att_val, inputFile, hist, outputFile=""):
	call = "./ncatted.sh %s %s %s %s %s %s %s %s" % (att_nm, var_nm, mode, att_type, att_val, inputFile, outputFile, hist)
	p = subprocess.Popen(shlex.split(call))
	out, err = p.communicate()
	if err: print(err)