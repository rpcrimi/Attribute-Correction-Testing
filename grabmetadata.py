# Open shell process
import subprocess
# Get current working directory
import os
# Split command line arguments
import shlex

def format_output(out):
	out = out.replace("\t", "").replace("\n", "").split(" ;")
	return filter(None, out)

currentDir = os.getcwd()
f = currentDir + '/test.nc'
p  = subprocess.Popen(['./ncdump.sh', f], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
p2 = subprocess.Popen(shlex.split('grep :standard_name'), stdin=p.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
p.stdout.close()
out, err = p2.communicate()

print format_output(out)