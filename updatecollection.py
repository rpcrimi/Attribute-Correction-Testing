import pymongo
import subprocess
import shlex

def run(collection, fileName):
	# Call mongoimport on collection using second argument as the file name
	call = "mongoimport -d Attribute_Correction -c %s %s" % (collection, fileName)
	p = subprocess.Popen(shlex.split(call), stdout=subprocess.PIPE, stderr=subprocess.PIPE)

	# Check for success or errors
	out, err = p.communicate()
	print (out or err)