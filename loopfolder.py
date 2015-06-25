import subprocess
import fnmatch
import os

currentDir = os.getcwd()
f = currentDir + '/netCDF'

matches = []
for root, dirnames, filenames in os.walk(currentDir):
  for filename in fnmatch.filter(filenames, '*.nc'):
    matches.append(os.path.join(root, filename))

for f in matches:
	p = subprocess.Popen(['./ncdump.sh', f], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	out, err = p.communicate()
	print(out or err)