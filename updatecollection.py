import pymongo
import sys
import json
import pprint
import subprocess
import shlex

def run(collection, fileName):
	connection        = pymongo.MongoClient()
	db                = connection["Attribute_Correction"]
	StandardNameFixes = db["StandardNameFixes"]
	VarNameFixes      = db["VarNameFixes"]

	# Call mongoimport on collection using second argument as the file name
	call = "mongoimport -d Attribute_Correction -c %s %s" % (collection, fileName)
	p = subprocess.Popen(shlex.split(call), stdout=subprocess.PIPE, stderr=subprocess.PIPE)

	# Check for success or errors
	out, err = p.communicate()
	print (out or err)