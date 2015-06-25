# Open shell process
import subprocess
# Get current working directory
import os
# Split command line arguments
import shlex
# Match filenames
import fnmatch

# Return formated output of grep call
def format_output(out):
	# Remove tabs and newlines and split on " ;"
	out = out.replace("\t", "").replace("\n", "").split(" ;")
	# Return list without empty elements
	return filter(None, out)

# Return a list of all netCDF files in "direrctory"
def get_nc_files(directory):
	matches = []
	# Do a walk through input directory
	for root, dirnames, filenames in os.walk(directory):
		# Find all filenames with .nc type
		for filename in fnmatch.filter(filenames, '*.nc'):
			# Add full path of netCDF file to matches list
			matches.append(os.path.join(root, filename))
	return matches

# Return a list of filenames and corresponding standard names in "ncFolder"
def get_standard_names(ncFolder):
	standardNames = []
	# Call ncdump and grep for :standard_name for each netCDF file in ncFolder
	for f in get_nc_files(ncFolder):
		p  = subprocess.Popen(['./ncdump.sh', f], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		p2 = subprocess.Popen(shlex.split('grep :standard_name'), stdin=p.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		p.stdout.close()
		out, err = p2.communicate()

		# If the file has a standard_name attribute add (filename, standard_names) tuple to standardNames list
		if out:
			standardNames.append((f, format_output(out)))
	
	return standardNames


currentDir = os.getcwd() + "/netCDF"

print get_standard_names(currentDir)