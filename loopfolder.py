import fnmatch
import os

currentDir = os.getcwd()
f = currentDir + '/netCDF'

def get_nc_files(dir):
	matches = []
	for root, dirnames, filenames in os.walk(currentDir):
		for filename in fnmatch.filter(filenames, '*.nc'):
			matches.append(os.path.join(root, filename))
	return matches

print get_nc_files(f)