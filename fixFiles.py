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
import mongoinit
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
def identify_attribute(var, attr, logFile, fileName, fixFlag, histFlag):
	# Check if (var, attr) is valid CF Standard Name pair
	cursor = db.CFVars.find_one({ '$and': [{"CF Standard Name": { '$eq': attr}}, {"Var Name": {'$eq': var}}]})
	# Log notification of correct attribute
	if (cursor):
		text = var + ":" + attr
		log(logFile, fileName, text, "Variable Confirmed")
		# Return true for confirming file
		return True

	# Standard Name exists in CF Standard Name collection
	cursor = db.CFVars.find({"CF Standard Name": { '$eq': attr}})
	if (cursor.count() != 0):
		# Check if (var, attr) pair is in VarNameFixes collection
		cursor = db.VarNameFixes.find_one({ '$and': [{"Incorrect Var Name": { '$eq': var}}, {"CF Standard Name": {'$eq': attr}}]})
		if (cursor):

			if fixFlag:
				ncrename.run(var, cursor["Known Fix"], fileName, histFlag)

			# Log the fix
			text = var + "," + cursor["Known Fix"] + "," + attr
			log(logFile, fileName, text, 'Switched Variable')
			# Return true for confirming file
			return False
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
		cursor = db.StandardNameFixes.find_one({ '$and': [{"Incorrect Var": { '$eq': attr}}, {"Var Name": {'$eq': var}}]})
		# If attr exists in StandardNameFixes collection
		if (cursor):

			if fixFlag:
				ncatted.run("standard_name", var, "o", "c", cursor["Known Fix"], fileName, histFlag)

			# Log the fix
			text = var + "," + attr + "," + cursor["Known Fix"]
			log(logFile, fileName, text, 'Switched Attribute')
			# Return true for confirming file
			return False
		# Get best N best estimates for "attr"
		else:
			bestEstimatesList = best_estimates(attr)
			bestEstimates = ""
			for e in bestEstimatesList:
				bestEstimates += str(e[0]) + " " + str(e[1]) + " | "
			text = var + "," + attr + "," + bestEstimates
			log(logFile, fileName, text, 'Estimated')
			# Return false for confirming file
			return False

	return

def fix_files(srcDir, dstDir, logFile, fixFlag, histFlag):
	# (filename, standard_name) list of all files in ncFolder
	standardNames = grabmetadata.get_standard_names(srcDir, dstDir)
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
			fileName = f[0]
			standNames = f[1]
			log(logFile, fileName, "", 'File Started')
			# If the file has no standard names, log the issue
			if not standNames:
				log(logFile, fileName, "", 'No Standard Names')
				fileFlag = False
			# For each attribute in standard_name list, format and identify attribute
			else:
				for attr in standNames:
					splitAttr = attr.split(":")
					flag = identify_attribute(splitAttr[0], splitAttr[1], logFile, fileName, fixFlag, histFlag)
					# Check if something in file was changed
					if flag == False:
						fileFlag = False
			# If file had no errors or KnownFix occured ==> Confirm file
			if fileFlag:
				if fixFlag:
					# New path for copying file
					dstdir = dstDir+os.path.dirname(fileName)
					# If path does not exist ==> create directory structure
					if not os.path.exists(dstdir):
						os.makedirs(dstdir)
					# Copy original file to dstdir
					shutil.move(fileName, dstdir)
				# Log the confirmed file
				log(logFile, fileName, "", 'File Confirmed')
			# Reset fileFlag
			fileFlag = True
			# Update progress bar
			bar.update(i)
			i = i + 1
		bar.finish()

def main():
	parser = argparse.ArgumentParser(description='Metadata Correction Algorithm')
	parser.add_argument("-o", "--op", "--operation", dest="operation",  help = "Operation to run (initDB, resetDB, updateCollection, fixFiles)", default="fixFiles")
	parser.add_argument("-c", "--collection",        dest="collection", help = "Collection to update")
	parser.add_argument("-u", "--updates",           dest="updates",    help = "JSON file containing updates")
	parser.add_argument("-s", "--srcDir",            dest="srcDir",     help = "Folder of nc or nc4 files to handle")
	parser.add_argument("-d", "--dstDir",            dest="dstDir",     help = "Folder to copy fixed files to")
	parser.add_argument("-l", "--logFile",           dest="logFile",    help = "File to log metadata changes to")
	parser.add_argument("-f", "--fixFlag",           dest="fixFlag",    help = "Flag to fix files or only report possible changes (-f = Fix Files)",  action='store_true', default=False)
	parser.add_argument("--hist", "--histFlag",      dest="histFlag",   help = "Flag to append changes to history metadata (-h = append to history)", action='store_true', default=False)
	
	args = parser.parse_args()

	if(len(sys.argv) == 1):
		parser.print_help()
	else:
		if args.operation == "initDB":
			mongoinit.run()
		elif args.operation == "resetDB":
			dropDB.run()
			mongoinit.run()
		elif args.operation == "updateCollection":
			if (args.collection and args.updates):
				updateCollection.run(args.collection, args.updates)
			else:
				parser.error("updateCollection requres collection and updates file")
		elif args.operation == "fixFiles":
			if (args.srcDir and args.dstDir and args.logFile):
				if args.histFlag:
					fix_files(args.srcDir, args.dstDir, args.logFile, args.fixFlag, "-h")
				else:
					fix_files(args.srcDir, args.dstDir, args.logFile, args.fixFlag, "")
			else:
				parser.error("fixFiles requires srcDirectory, dstDirectory, and logFile")

if __name__ == "__main__":
	main()