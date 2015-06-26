import pymongo
from bson.objectid import ObjectId
import attributecorrector
from difflib import SequenceMatcher
import logging
import os
import grabmetadata

connection        = pymongo.MongoClient()
db                = connection["Attribute_Correction"]
CFVars            = db["CFVars"]
StandardNameFixes = db["StandardNameFixes"]
VarNameFixes      = db["VarNameFixes"]
logFile           = os.getcwd() + "/results.log"
	

# Log info in "logFile" for file "fileName"
def log(logFile, fileName, text, logType):
	logging.basicConfig(level=logging.DEBUG, format='%(levelname)-8s %(message)s', filename=logFile, filemode='w')
	if logType == 'File Started':
		logging.info("--------------------------------------------------------------------------------------------------------------------------------------")
		logging.debug("Starting in file [%s]:", fileName)

	elif logType == 'File Confirmed':
		logging.info("Confirmed file: [%s]", fileName)

	elif logType == 'Variable Confirmed':
		logging.info("Standard Name [%s] confirmed", text)

	elif logType == 'Switched Attribute':
		splitText = text.split(",")
		attr      = splitText[0]
		oldName   = splitText[1]
		newName   = splitText[2]
		logging.info("Switched [%s] standard_name from [%s] to [%s]", attr, oldName, newName)

	elif logType == 'Switched Variable':
		splitText = text.split(",")
		oldName   = splitText[0]
		newName   = splitText[1]
		attr      = splitText[2]
		logging.info("Switched variable name from [%s:%s] to [%s:%s]", oldName, attr, newName, attr)


	elif logType == 'Estimated':
		splitText    = text.split(",")
		var          = splitText[0]
		wrongAttr    = splitText[1]
		numEstimates = splitText[2]
		estimates    = splitText[3]
		logging.debug("Standard Name [%s:%s] best %s estimates: %s", var, wrongAttr, numEstimates, estimates)

	elif logType == 'No Standard Names':
		logging.debug("[%s]: no standard names defined", fileName)

	elif logType == 'No Matching Var Name':
		splitText = text.split(",")
		attr = splitText[0]
		recommendations = splitText[1]
		logging.debug("[%s] recommended Variable Names: %s", attr, recommendations)

# Return list of CF Standard Names from CFVars Collection
def get_CF_Standard_Names():
	# Query CFVars for all Variables
	cursor = db.CFVars.find()
	CFStandards = []
	# Append each CF STandard Name to CFStandards list
	for attr in cursor:
		CFStandards.append(attr["CF Standard Name"])
	return CFStandards

# Return similarity ratio of string "a" and "b"
def similar(a,b):
	a = a.lower()
	b = b.lower()
	return SequenceMatcher(None, a, b).ratio()

# Return the "N" # of CF Standard Vars with the most similarity to "wrongAttr"
def best_estimates(wrongAttr, N):
	# Grab CF Standard Names
	CFStandards = get_CF_Standard_Names()
	similarities = []
	# Calculate percent difference between the wrong attribute and each CF Standard Name
	# Append (attr, percentOff) tuple to similarities for future sorting
	for attr in CFStandards:
		percentOff = similar(wrongAttr, attr)
		similarities.append((attr, percentOff))
	# Sort similarities list by second element in tuple
	similarities.sort(key=lambda x: x[1])
	# If N is less than size of similarities return last N elements
	if len(similarities) >= N:
		# Reverse order so top match is first in list
		return list(reversed(similarities[-N:]))
	# Else return full list
	else:
		return list(reversed(similarities))

# Return validation of correct attribute
# or corrected attribute from Known fixes collection
# or return the top "N" matches from CFVars collection
def identify_attribute(var, attr, N, logFile, fileName):
	# Check if (var, attr) is valid CF Standard Name pair
	cursor = db.CFVars.find({ '$and': [{"CF Standard Name": { '$eq': attr}}, {"Var Name": {'$eq': var}}]})
	
	# If (var,attr) pair exists in CF Standards collection ==> log notification of correct attribute
	if (cursor.count() != 0):
		text = var + ":" + attr
		log(logFile, fileName, text, "Variable Confirmed")
		# Return true for confirming file
		return True

	# Standard Name exists but input variable does not match
	# Log recommendations for variables corresponding to the CF Standard Name
	elif (db.CFVars.find({"CF Standard Name": { '$eq': attr}}).count() != 0):
		cursor = db.VarNameFixes.find({ '$and': [{"Incorrect Var Name": { '$eq': var}}, {"CF Standard Name": {'$eq': attr}}]})
		# If (var, attr) pair is in VarNameFixes collection, switch 
		if (cursor.count() != 0):
			# Grab id, times seen, and var name of known fix document
			_id       = cursor[0]["_id"]
			timesSeen = cursor[0]["Times Seen"]

			# Update the times seen value by adding 1
			db.VarNameFixes.update({"_id": _id}, {"$set": {"Times Seen": timesSeen + 1}})

			# Log the fix
			text = var + "," + cursor[0]["Known Fix"] + "," + attr
			log(logFile, fileName, text, 'Switched Variable')
			# Return true for confirming file
			return True
		else:
			cursor = db.CFVars.find({"CF Standard Name": { '$eq': attr}})
			recommendations = var + ":" + attr + ","
			for var in cursor:
				recommendations += var["Var Name"] + " | "
			log(logFile, fileName, recommendations, 'No Matching Var Name')
			# Return false for confirming file
			return False

	# attr does not exist in CF Standards
	else:
		# Set all characters to lowercase
		attr = attr.lower()
		# Check if KnownFixes has seen this error before
		cursor = db.StandardNameFixes.find({ '$and': [{"Incorrect Var": { '$eq': attr}}, {"Var Name": {'$eq': var}}]})
		# If attr exists in KnownFixes
		if (cursor.count() != 0):
			# Grab id, times seen, and var name of known fix document
			_id       = cursor[0]["_id"]
			timesSeen = cursor[0]["Times Seen"]

			# Update the times seen value by adding 1
			db.StandardNameFixes.update({"_id": _id}, {"$set": {"Times Seen": timesSeen + 1}})

			# Log the fix
			text = var + "," + attr + "," + cursor[0]["Known Fix"]
			log(logFile, fileName, text, 'Switched Attribute')
			# Return true for confirming file
			return True
		# Get best N best estimates for "attr"
		else:
			bestEstimatesList = best_estimates(attr, N)
			bestEstimates = ""
			for e in bestEstimatesList:
				bestEstimates += str(e[0]) + " " + str(e[1]) + " | "
			text = var + "," + attr + "," + str(N) + "," + bestEstimates
			log(logFile, fileName, text, 'Estimated')
			# Return false for confirming file
			return False

	return

def fix_files(folder, logFile):
	# Full path of folder that contains netCDF files
	ncFolder = os.getcwd() + "/" + folder
	# (filename, standard_name) list of all files in ncFolder
	standardNames = grabmetadata.get_standard_names(ncFolder)

	# Flag for confirming file
	fileFlag = True
	# For each file in the list, log the file has started
	for f in standardNames:
		log(logFile, f[0], "", 'File Started')
		# If the file has no standard names, log the issue
		if not f[1]:
			log(logFile, f[0], "", 'No Standard Names')
			fileFlag = False
		# For each attribute in standard_name list, format and identify attribute
		else:
			for attr in f[1]:
				splitAttr = attr.replace("standard_name = ", "").replace("\"", "").split(":")
				flag = identify_attribute(splitAttr[0], splitAttr[1], 3, logFile, f[0])
				if flag == False:
					fileFlag = False
		if fileFlag:
			log(logFile, f[0], "", 'File Confirmed')
		fileFlag = True

fix_files("ncFiles", logFile)

