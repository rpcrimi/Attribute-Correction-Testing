import pymongo
from bson.objectid import ObjectId
import attributecorrector
from difflib import SequenceMatcher
import logging
import os
import grabmetadata

connection = pymongo.MongoClient()
db = connection["Attribute_Correction"]
CFVars = db["CFVars"]
KnownFixes = db["KnownFixes"]
logFile = os.getcwd() + "/results.log"

# Log info in "logFile" for file "fileName"
def log(logFile, fileName, text, logType):
	logging.basicConfig(level=logging.DEBUG, format='%(levelname)-8s %(message)s', filename=logFile, filemode='w')
	if logType == 'File Started':
		logging.info("--------------------------------------------------------------------------------------------------------------------------------------")
		logging.info("Starting in file [" + fileName + "]:")

	elif logType == 'File Confirmed':
		logging.info("Completed file: [" + fileName + "]")

	elif logType == 'Variable Confirmed':
		logging.info("Standard Name [" + text + "] confirmed")

	elif logType == 'Switched Attribute':
		splitText = text.split(",")
		var       = splitText[0]
		oldName   = splitText[1]
		newName   = splitText[2]
		logging.info
		logging.info("Switched [" + var + "] standard_name from [" + oldName + "] to [" + newName + "]")

	elif logType == 'Estimated':
		splitText    = text.split(",")
		var          = splitText[0]
		wrongAttr    = splitText[1]
		numEstimates = splitText[2]
		estimates    = splitText[3]
		logging.debug("[" + var + ":" + wrongAttr + "] best " + numEstimates + " estimates: " + estimates)

	elif logType == 'No Standard Names':
		logging.debug("[" + fileName + "]: no standard names defined")

	elif logType == 'No Matching Var Name':
		splitText = text.split(",")
		attr = splitText[0]
		recommendations = splitText[1]
		logging.debug("[" + attr + "] recommended Variable Names: " + recommendations)

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
	# Check if attr is valid CF Standard Name
	cursor = db.CFVars.find({ '$and': [{"CF Standard Name": { '$eq': attr}}, {"Var Name": {'$eq': var}}]})
	
	# If (var,attr) pair exists in CF Standards collection ==> log notification of correct attribute
	if (cursor.count() != 0):
		text = var + ":" + attr
		log(logFile, fileName, text, "Variable Confirmed")

	# Standard Name exists but input variable does not match
	elif (db.CFVars.find({"CF Standard Name": { '$eq': attr}}).count() != 0):
		cursor = db.CFVars.find({"CF Standard Name": { '$eq': attr}})
		recommendations = var + ":" + attr + ","
		for var in cursor:
			recommendations += var["Var Name"] + " | "
		log(logFile, fileName, recommendations, 'No Matching Var Name')

	# attr does not exist in CF Standards
	else:
		# Set all characters to lowercase
		attr = attr.lower()
		# Check if KnownFixes has seen this error before
		cursor = db.KnownFixes.find({ '$and': [{"Incorrect Var": { '$eq': attr}}, {"Var Name": {'$eq': var}}]})
		# If attr exists in KnownFixes
		if (cursor.count() != 0):
			# Grab id, times seen, and var name of known fix document
			_id       = cursor[0]["_id"]
			timesSeen = cursor[0]["Times Seen"]

			# Update the times seen value by adding 1
			db.KnownFixes.update({"_id": _id}, {"$set": {"Times Seen": timesSeen + 1}})

			# Log the fix
			text = var + "," + attr + "," + cursor[0]["Known Fix"]
			log(logFile, fileName, text, 'Switched Attribute')
			# Return the known fix for the incorrect attr
			return cursor[0]["Known Fix"]
		# Get best N best estimates for "attr"
		else:
			bestEstimatesList = best_estimates(attr, N)
			bestEstimates = ""
			for e in bestEstimatesList:
				bestEstimates += str(e[0]) + " " + str(e[1]) + " | "
			text = var + "," + attr + "," + str(N) + "," + bestEstimates
			log(logFile, fileName, text, 'Estimated')

	return

def fix_files(folder, logFile):
	ncFolder = os.getcwd() + "/" + folder
	standardNames = grabmetadata.get_standard_names(ncFolder)
	for f in standardNames:
		log(logFile, f[0], "", 'File Started')
		if not f[1]:
			log(logFile, f[0], "", 'No Standard Names')
		else:
			for attr in f[1]:
				splitAttr = attr.replace("standard_name = ", "").replace("\"", "").split(":")
				identify_attribute(splitAttr[0], splitAttr[1], 3, logFile, f[0])

fix_files("ncFiles", logFile)

