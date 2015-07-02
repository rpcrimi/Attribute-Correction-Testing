import pymongo
from difflib import SequenceMatcher
import logging
import os
import shutil
import ntpath
import argparse
from progressbar import *
import grabmetadata
import ncatted
import ncrename
import dropDB
import mongoInit
import updateCollection

connection        = pymongo.MongoClient()
db                = connection["Attribute_Correction"]
CFVars            = db["CFVars"]
StandardNameFixes = db["StandardNameFixes"]
VarNameFixes      = db["VarNameFixes"]
	

# Log info in "logFile" for file "fileName"
def log(logFile, fileName, text, logType):
	logging.basicConfig(level=logging.DEBUG, format='%(levelname)-8s %(message)s', filename=logFile, filemode='w')
	if logType == 'File Started':
		logging.info("--------------------------------------------------------------------------------------------------------------------------------------")
		logging.debug("Starting in file [%s]:", fileName)

	elif logType == 'File Confirmed':
		logging.info("Confirmed file: [%s]", fileName)

	elif logType == 'File Moved':
		logging.info("Moved file from [%s] to [%s]", fileName, text)

	elif logType == 'File Copied':
		logging.info("Copied file from [%s] to [%s]", fileName, text)

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
		estimates    = splitText[2]
		logging.debug("Standard Name [%s:%s] best 3 estimates: %s", var, wrongAttr, estimates)

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
	return SequenceMatcher(None, a, b).ratio()*100

# Return the "N" # of CF Standard Vars with the most similarity to "wrongAttr"
def best_estimates(wrongAttr):
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

	return list(reversed(similarities[-3:]))

# Return validation of correct attribute
# or corrected attribute from Known fixes collection
# or return the top 3 matches from CFVars collection
def identify_attribute(var, attr, logFile, fileInfo, stgDir, fixFlag):
	# Check if (var, attr) is valid CF Standard Name pair
	cursor = db.CFVars.find_one({ '$and': [{"CF Standard Name": { '$eq': attr}}, {"Var Name": {'$eq': var}}]})
	# Log notification of correct attribute
	if (cursor):
		text = var + ":" + attr
		log(logFile, fileInfo["fullPath"], text, "Variable Confirmed")
		# Return true for confirming file
		return True

	# Standard Name exists in CF Standard Name collection
	cursor = db.CFVars.find({"CF Standard Name": { '$eq': attr}})
	if (cursor.count() != 0):
		# Check if (var, attr) pair is in VarNameFixes collection
		cursor = db.VarNameFixes.find_one({ '$and': [{"Incorrect Var Name": { '$eq': var}}, {"CF Standard Name": {'$eq': attr}}]})
		if (cursor):
			if fixFlag:
				# File is not in staging directory
				if fileInfo["folder"] != stgDir:
					# Create copy of folder structure in staging directory
					stgDir = stgDir+os.path.dirname(fileInfo["fullPath"])
					if not os.path.exists(stgDir):
						os.makedirs(stgDir)
					ncrename.run(var, cursor["Known Fix"], fileInfo["fullPath"], (stgDir+"/"+ntpath.basename(fileInfo["fullPath"])))
				else:
					ncrename.run(var, cursor["Known Fix"], fileInfo["fullPath"])

			# Log the fix
			text = var + "," + cursor["Known Fix"] + "," + attr
			log(logFile, fileInfo["fullPath"], text, 'Switched Variable')
			# Return true for confirming file
			return False
		else:
			cursor = db.CFVars.find({"CF Standard Name": { '$eq': attr}})
			recommendations = var + ":" + attr + ","
			for var in cursor:
				recommendations += var["Var Name"] + " | "
			log(logFile, fileInfo["fullPath"], recommendations, 'No Matching Var Name')
			# Return false for confirming file
			return False

	# attr does not exist in CF Standards
	else:
		# Set all characters to lowercase
		attr = attr.lower()
		# Check if (var, attr) piar is in StandardNameFixesclear

		cursor = db.StandardNameFixes.find_one({ '$and': [{"Incorrect Standard Name": { '$eq': attr}}, {"Var Name": {'$eq': var}}]})
		# If attr exists in StandardNameFixes collection
		if (cursor):
			if fixFlag:
				# File is not in staging directory
				if fileInfo["folder"] != stgDir:
					# Create copy of folder structure in staging directory
					stgDir = stgDir+os.path.dirname(fileInfo["fullPath"])
					if not os.path.exists(stgDir):
						os.makedirs(stgDir)
					ncatted.run("standard_name", var, "o", "c", cursor["Known Fix"], "-h", fileInfo["fullPath"], (stgDir+"/"+ntpath.basename(fileInfo["fullPath"])))
				else:
					ncatted.run("standard_name", var, "o", "c", cursor["Known Fix"], "-h", fileInfo["fullPath"])

			# Log the fix
			text = var + "," + attr + "," + cursor["Known Fix"]
			log(logFile, fileInfo["fullPath"], text, 'Switched Attribute')
			# Return true for confirming file
			return True
		# Get best N best estimates for "attr"
		else:
			bestEstimatesList = best_estimates(attr)
			bestEstimates = ""
			for e in bestEstimatesList:
				bestEstimates += str(e[0]) + " " + str(e[1]) + " | "
			text = var + "," + attr + "," + bestEstimates
			log(logFile, fileInfo["fullPath"], text, 'Estimated')
			# Return false for confirming file
			return False

	return

def get_file_info(f):
	fileName   = f[0]
	standNames = f[1]

	info = {}
	info["standNames"] = standNames
	info["fileName"]   = ntpath.basename(fileName)
	info["folder"]     = fileName.split("/")[0]+"/"
	info["path"]       = os.path.dirname(fileName)
	info["fullPath"]   = fileName

	return info

def fix_files(srcDir, stgDir, dstDir, logFile, fixFlag):
	# (filename, standard_name) list of all files in ncFolder
	standardNames = grabmetadata.run(srcDir, stgDir, dstDir)
	if standardNames:
		# Number of files for use in progress bar
		totalFiles = len(standardNames)
		i = 1
		widgets = ['Percent Done: ', Percentage(), ' ', AnimatedMarker(), ' ', ETA()]
		bar = ProgressBar(widgets=widgets, maxval=totalFiles).start()
		# Flag for confirming file
		fileFlag = True
		# For each file in the list, log the file has started
		for f in standardNames:
			fileInfo = get_file_info(f)

			log(logFile, fileInfo["fullPath"], "", 'File Started')
			# If the file has no standard names, log the issue
			if not fileInfo["standNames"]:
				log(logFile, fileInfo["fullPath"], "", 'No Standard Names')
				fileFlag = False
			# For each attribute in standard_name list, format and identify attribute
			else:
				for attr in fileInfo["standNames"]:
					splitAttr = attr.split(":")
					flag = identify_attribute(splitAttr[0], splitAttr[1], logFile, fileInfo, stgDir, fixFlag)
					# Check if something in file was changed
					if flag == False:
						fileFlag = False
			# If file had no errors or KnownFix occured ==> Confirm file
			if fileFlag:
				# Log the confirmed file
				log(logFile, fileInfo["fullPath"], "", 'File Confirmed')
				
				if fixFlag:
					# New path for copying file
					dstDirectory = dstDir + os.path.dirname(fileInfo["fullPath"]).replace(stgDir, "")
					# If path does not exist ==> create directory structure
					if not os.path.exists(dstDirectory):
						os.makedirs(dstDirectory)
					# Copy original file to dstdir
					if fileInfo["folder"] == stgDir:
						log(logFile, fileInfo["fullPath"], dstDirectory, 'File Moved')
						shutil.move(fileInfo["fullPath"], dstDirectory)
					else:
						log(logFile, fileInfo["fullPath"], dstDirectory, 'File Copied')
						shutil.copy(fileInfo["fullPath"], dstDirectory)
			# Reset fileFlag
			fileFlag = True
			# Update progress bar
			bar.update(i)
			i = i + 1
		bar.finish()

def main():
	parser = argparse.ArgumentParser(description='Metadata Correction Algorithm')
	parser.add_argument("-o", "--op", "--operation",   dest="operation",  help = "Operation to run (initDB, resetDB, updateCollection, fixFiles)",        default="fixFiles")
	parser.add_argument("-c", "--collection",          dest="collection", help = "Collection to update")
	parser.add_argument("-u", "--updates",             dest="updates",    help = "JSON file containing updates")
	parser.add_argument("-s", "--srcDir",              dest="srcDir",     help = "Folder of nc or nc4 files to handle")
	parser.add_argument("-e", "--editDir", "--stgDir", dest="stgDir",     help = "Folder to store edited files in")
	parser.add_argument("-d", "--dstDir",              dest="dstDir",     help = "Folder to copy fixed files to")
	parser.add_argument("-l", "--logFile",             dest="logFile",    help = "File to log metadata changes to")
	parser.add_argument("-f", "--fixFlag",             dest="fixFlag",    help = "Flag to fix files or only report possible changes", action='store_true', default=False)
	args = parser.parse_args()

	if(len(sys.argv) == 1):
		parser.print_help()
	else:
		if args.operation == "initDB":
			mongoInit.run()
		elif args.operation == "resetDB":
			dropDB.run()
			mongoInit.run()
		elif args.operation == "updateCollection":
			if (args.collection and args.updates):
				updateCollection.run(args.collection, args.updates)
			else:
				parser.error("updateCollection requres collection and updates file")
		elif args.operation == "fixFiles":
			if (args.srcDir and args.stgDir and args.dstDir and args.logFile):
				fix_files(args.srcDir, args.stgDir, args.dstDir, args.logFile, args.fixFlag)
			else:
				parser.error("fixFiles requires srcDirectory, editDirectory, dstDirectory, and logFile")

if __name__ == "__main__":
	main()
