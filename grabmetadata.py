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
def get_nc_files(directory, dstFolder):
	matches = []
	# Do a walk through input directory
	for root, dirnames, files in os.walk(directory):
		# Find all filenames with .nc type
		for filename in files:
				if filename.endswith(('.nc', '.nc4')):
					filename = os.path.join(root, filename)
					dstFileName = dstFolder + filename
					if not os.path.isfile(dstFileName):
						# Add full path of netCDF file to matches list
						matches.append(filename)
	return matches

# Return a list of filenames and corresponding standard names in "ncFolder"
def get_standard_names(ncFolder, dstFolder):
	standardNames = []
	# Call ncdump and grep for :standard_name for each netCDF file in ncFolder
	for f in get_nc_files(ncFolder, dstFolder):
		p  = subprocess.Popen(['./ncdump.sh', f], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		p2 = subprocess.Popen(shlex.split('grep :standard_name'), stdin=p.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		p.stdout.close()
		out, err = p2.communicate()
		standardNames.append((f, format_output(out)))
	
	return standardNames