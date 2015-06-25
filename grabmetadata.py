# Open shell process
import subprocess
# Get current working directory
import os
# Split command line arguments
import shlex
# Match filenames
import fnmatch

def format_output(out):
	out = out.replace("\t", "").replace("\n", "").split(" ;")
	return filter(None, out)

def get_nc_files(dir):
	matches = []
	for root, dirnames, filenames in os.walk(currentDir):
		for filename in fnmatch.filter(filenames, '*.nc'):
			matches.append(os.path.join(root, filename))
	return matches

def get_standard_names(ncFolder):
	for f in get_nc_files(ncFolder):
		p  = subprocess.Popen(['./ncdump.sh', f], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		p2 = subprocess.Popen(shlex.split('grep :standard_name'), stdin=p.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		p.stdout.close()
		out, err = p2.communicate()

		print format_output(out)


currentDir = os.getcwd() + "/NOAA-GFDL"
print get_standard_names(currentDir)