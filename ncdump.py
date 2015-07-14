# Open shell process
import subprocess
# Split command line arguments
import shlex

def run(fileName):
	call = "./ncdump.sh %s" % (fileName)
	p = subprocess.Popen(shlex.split(call), stdout=subprocess.PIPE)
	out, err = p.communicate()
	p.stdout.close()
	if err: print(err)
	else: return out