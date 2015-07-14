import pymongo
import subprocess
import shlex
import re
import argparse
import os
import logging
import sys
import datetime
import ncatted
import ncdump
import pprint

connection        = pymongo.MongoClient()
db                = connection["Attribute_Correction"]
CFVars            = db["CFVars"]
ValidFreq         = db["ValidFreq"]
FreqFixes         = db["FreqFixes"]
realizationRegex  = re.compile('r[0-9]+i[0-9]+p[0-9]+')

def get_datetime(): return str(datetime.datetime.now()).split(".")[0].replace(" ", "T")

def get_logfile(src): return (src+"_"+get_datetime()+".log")

# Log info in "logFile" for file "fileName"
def log(logFile, fileName, text, logType):
	logging.basicConfig(level=logging.DEBUG, format='%(levelname)-8s %(message)s', filename=logFile, filemode='w')
	if logType == 'File Started':
		logging.info("-" * 100)
		logging.debug("Starting file name: [%s]", fileName)

	elif logType == 'File Confirmed':
		logging.debug("Confirmed file name: [%s]", fileName)
		logging.info("-" * 100)

	elif logType == 'Var Error':
		logging.debug("Variable [%s] not recognized", text)

	elif logType == 'Freq Error':
		logging.debug("Frequency [%s] not recognized", text)

	elif logType == 'Model Error':
		logging.debug("Model [%s] not recognized", text)	

	elif logType == 'Realization Error':
		logging.debug("Metadata Realization [%s] does not match File Name Realization [%s]", text[0], text[1])

	elif logType == 'File Name Error':
		logging.debug("File Name [%s] does not match created File Name [%s]", text[0], text[1])

	elif logType == 'Renamed File Name':
		logging.debug("File Name [%s] renamed to [%s]", fileName, text)

	elif logType == 'Realization Fix':
		logging.debug("Metadata Realization changed from [%s] to [%s]", text[0], text[1])

	elif logType == 'Var Name Fix':
		logging.debug("Variable [%s] changed to [%s]", text[0], text[1])

	elif logType == 'Renamed Var Folder':
		logging.debug("Folder [%s] renamed to [%s]", text[0], text[1])

	elif logType == 'Renamed Freq Folder':
		logging.debug("Renamed Frequency folder name [%s] to [%s]", text[0], text[1])

	elif logType == 'Metadata Fix':
		logging.debug("[%s] in metadata changed from [%s] to [%s]", text[0], text[1], text[2])

# Get model, initialization date, frequency, and variable from the full path of the given file
def get_path_info(fullPath):
	dictionary = {}
	splitFileName = fullPath.split("/")
	if splitFileName[0] == 'NOAA-GFDL' or splitFileName[0] == 'CCCMA':
		dictionary["model_id"]          = splitFileName[1]
		dictionary["initDate"]          = splitFileName[2]
		dictionary["frequency"]         = splitFileName[3]
		dictionary["modeling_realm"]    = splitFileName[5]
		dictionary["variable"]          = splitFileName[6]
		dictionary["fileName"]          = os.path.basename(fullPath)
		dictionary["fullPath"]          = fullPath
		dictionary["splitFileName _"]   = fullPath.split("_")
		dictionary["splitFileName ."]   = fullPath.split(".")
	elif splitFileName[0] == 'UM-RSMAS' or splitFileName[0] == 'NASA-GMAO':
		dictionary["model_id"]          = splitFileName[1]
		dictionary["initDate"]          = splitFileName[2]
		dictionary["frequency"]         = splitFileName[3]
		dictionary["modeling_realm"]    = splitFileName[4]
		dictionary["variable"]          = splitFileName[5]
		dictionary["fileName"]          = os.path.basename(fullPath)
		dictionary["fullPath"]          = fullPath
		dictionary["splitFileName _"]   = fullPath.split("_")
		dictionary["splitFileName ."]   = fullPath.split(".")
	return dictionary

# Create dictionary of given info from command line
def create_dict_given_info(model=None, initDate=None, freq=None, var=None):
	dictionary = {}
	if model:
		dictionary["model"]    = model
	if initDate:
		dictionary["initDate"] = initDate
	if freq:
		dictionary["freq"]     = freq
	if var:
		dictionary["var"]      = var
	return dictionary

# Return a list of all netCDF files in "direrctory"
def get_nc_files(directory):
	matches = []
	# Do a walk through input directory
	for root, dirnames, files in os.walk(directory):
		# Find all filenames with .nc type
		for filename in files:
			filename = os.path.join(root, filename)
			if filename.endswith(('.nc', '.nc4')):
					matches.append(filename)
	return matches

# Grab the attribute = attr from the file = fullPath
# This should only be used for global attributes
def get_metadata(pathDict, attr):
	# Create the grep string
	grep = 'grep :'+attr
	dump = './ncdump.sh ' + pathDict["fullPath"]
	# Dump metadata and grep for attribute
	p  = subprocess.Popen(shlex.split(dump), stdout=subprocess.PIPE)
	p2 = subprocess.Popen(shlex.split(grep), stdin=p.stdout, stdout=subprocess.PIPE)
	p.stdout.close()
	out, err = p2.communicate()
	p2.stdout.close()
	# Format metadata by removing tabs, newlines, and semicolons and grabbing the value
	# lstrip("0") for realization numbers of the form r01i1p1
	metadata = out.replace("\t", "").replace("\n", "").replace(" ;", "").split(" = ")[1].strip('"').lstrip("0")
	return metadata

def dump_metadata(pathDict, metadataFolder):
	out = ncdump.run(pathDict["fullPath"])
	dstDir = metadataFolder+os.path.dirname(pathDict["fileName"])
	# If path does not exist ==> create directory structure
	if not os.path.exists(dstDir):
		os.makedirs(dstDir)

	fileName = metadataFolder+pathDict["fileName"].replace(".nc", "").rstrip("4")+".txt"
	with open(fileName, "w") as text_file:
		text_file.write(out)

def validate_variable(pathDict, logFile, fixFlag):
	if not db.CFVars.find_one({"Var Name": pathDict["variable"]}):
		# Try to fix the variable name by making characters lowercase
		#------------------------------------------------------------
		if db.CFVars.find_one({"Var Name": pathDict["variable"].lower()}):
			if fixFlag:
				pathDict["variable"] = pathDict["variable"].lower()
			log(logFile, pathDict["fileName"], [pathDict["variable"].upper(), pathDict["variable"].lower()], 'Var Name Fix')

			# Fix the folder that is named after the variable
			#------------------------------------------------
			oldDir      = os.path.dirname(pathDict["fullPath"])
			# Get the name of the variable folder
			parDirIndex = oldDir.rfind('/')
			parDir      = oldDir[parDirIndex+1:]
			# If folder is uppercase ==> make lowercase and rename folder
			if parDir.isupper():
				parDir = parDir.lower()
				newDir = oldDir[:parDirIndex+1]+parDir
				if fixFlag:
					os.rename(oldDir, newDir)
				log(logFile, pathDict["fileName"], [oldDir, newDir], 'Renamed Var Folder')
		else:
			log(logFile, pathDict["fileName"], pathDict["variable"], 'Var Error')
		# Error seen	
		return False
	else:
		return True

def validate_frequency(pathDict, logFile, fixFlag):
	if not db.ValidFreq.find_one({"Frequency": pathDict["frequency"]}):
		
		cursor = db.FreqFixes.find_one({"Incorrect Freq": pathDict["frequency"]})
		if cursor:
			# Rename Frequency folder
			oldDir = pathDict["fullPath"].split(pathDict["frequency"])[0]+pathDict["frequency"]+"/"
			newDir = pathDict["fullPath"].split(pathDict["frequency"])[0]+cursor["Known Fix"]+"/"
			if fixFlag:
				os.rename(oldDir, newDir)
			log(logFile, pathDict["fileName"], [pathDict["frequency"], cursor["Known Fix"]], 'Renamed Freq Folder')
			pathDict["fullPath"] = pathDict["fullPath"].replace(pathDict["frequency"], cursor["Known Fix"])
			pathDict["frequency"] = cursor["Known Fix"]

		else:
			log(logFile, pathDict["fileName"], pathDict["frequency"], 'Freq Error')
		# Error seen
		return False
	else:
		return True

def validate_realization(pathDict, logFile, fixFlag):
	pathDict["fileNameRealization"] = [match for match in pathDict["splitFileName _"] if re.match(realizationRegex, match)][0].replace(".nc", "").rstrip("4")
	# Grab realization number from metadata
	realization = "r"+get_metadata(pathDict, "realization")+"i1p1"
	# If the two values differ ==> fix the value in metadata to reflect filename
	if realization != pathDict["fileNameRealization"]:
		log(logFile, pathDict["fileName"], [realization, pathDict["fileNameRealization"]], 'Realization Error')
		if fixFlag:
			# Find the number after "r" in fileNameRealization value
			realizationNum = map(int, re.findall(r'\d+', pathDict["fileNameRealization"]))[0]
			# Overwrite realization value in metadata
			if fixFlag:
				ncatted.run("realization", "global", "o", "i", realizationNum, pathDict["fullPath"], ("-h" if histFlag else ""))
			log(logFile, pathDict["fileName"], [realization, pathDict["fileNameRealization"]], 'Realization Fix')
		# Error seen
		return False
	else:
		return True

def validate_metadata(pathDict, logFile, fixFlag):
	flag = True
	for meta in ["frequency", "model_id", "modeling_realm"]:
		metadataFreq = get_metadata(pathDict, meta)
		if metadataFreq != pathDict[meta]:
			if fixFlag:
				ncatted.run(meta, "global", "o", "c", pathDict[meta], pathDict["fullPath"], ("-h" if histFlag else ""))
			log(logFile, pathDict["fileName"], [meta, metadataFreq, pathDict[meta]], 'Metadata Fix')
			flag = False
	return flag

# Fix the file = fullPath
# pathDict contains info from file path
# Changes will occur if fixFlag == True
# Changes will appear in history if histFlag == False
def fix_filename(pathDict, logFile, fixFlag, histFlag):
	flag          = True

	varFlag         = validate_variable(pathDict, logFile, fixFlag)
	freqFlag        = validate_frequency(pathDict, logFile, fixFlag)
	realizationFlag = validate_realization(pathDict, logFile, fixFlag)
	metadataFlag    = validate_metadata(pathDict, logFile, fixFlag)
	if not (varFlag and freqFlag and realizationFlag and metadataFlag):
		flag = False

	# Create End Date and File Extension
	#-----------------------------------
	extension     = pathDict["splitFileName ."][-1]
	rootFileName  = ".".join(pathDict["splitFileName ."][0:-1])
	# If end of filename is not a realization number ==> it contains a start-end date
	if not re.match(realizationRegex, rootFileName.split("_")[-1]):
		# Grab the enddate from the file
		# TODO: FIX FOR NON 8 CHARACTER DATES
		endDate  = rootFileName[-8:]
		startEnd = pathDict["initDate"] + "-" + endDate
	# Do not append start-end date if none is provided
	else:
		startEnd = ""

	# Create filename based on pulled information
	#--------------------------------------------
	newFileName = pathDict["variable"]+"_"+pathDict["frequency"]+"_"+pathDict["model_id"]+"_"+pathDict["initDate"]+"_"+pathDict["fileNameRealization"]+(("_"+startEnd) if startEnd else "")+"."+extension
	# If filename differs from created filename ==> rename file to created filename
	if pathDict["fileName"] != newFileName:
		log(logFile, pathDict["fileName"], [pathDict["fileName"], newFileName], 'File Name Error')
		if fixFlag:
			newFullPath = os.path.dirname(pathDict["fullPath"])+"/"+newFileName
			os.rename(pathDict["fullPath"], newFullPath)
			log(logFile, pathDict["fileName"], newFileName, 'Renamed File Name')
		# Error seen
		flag = False

	# Return boolean value if error found
	return flag

def main():
	parser = argparse.ArgumentParser(description='File Name Correction Algorithm')
	parser.add_argument("-s", "--src", "--srcDir", dest="srcDir",         help = "Source Directory")
	parser.add_argument("-f", "--fileName",        dest="fileName",       help = "File Name for single file fix")
	parser.add_argument("--metadata",              dest="metadataFolder", help = "Folder to dump original metadata to", required=True)
	parser.add_argument("-m", "--model",           dest="model",          help = "Name of model (ex: NOAA-GFDL, CCSM4). This argument will overwrite any found model in metadata, path, or filename")
	parser.add_argument("-i", "--initDate",        dest="initDate",       help = "Initialization date (ex: 20090101). This argument will overwrite any found initDate in metadata, path, or filename")
	parser.add_argument("--freq", "--frequency",   dest="freq",           help = "Frequency (ex: day, mon, Omon). This argument will overwrite any found frequency in metadata, path, or filename")
	parser.add_argument("-v", "--var",             dest="var",            help = "Variable name (ex: pr, tasmax, hus). This argument will overwrite any found variable in metadata, path, or filename")
	parser.add_argument("-l", "--logFile",         dest="logFile",        help = "File to log metadata changes to")
	parser.add_argument("--fix", "--fixFlag",      dest="fixFlag",        help = "Flag to fix file names or only report possible changes (-f = Fix File Names)",  action='store_true', default=False)
	parser.add_argument("--hist", "--histFlag",    dest="histFlag",       help = "Flag to append changes to history metadata (-h = do not append to history)",           action='store_true', default=True)

	args = parser.parse_args()
	if(len(sys.argv) == 1):
		parser.print_help()

	else:
		logFile  = get_logfile(args.logFile if args.logFile else (args.srcDir.replace("/","") if args.srcDir else args.fileName))
		givenArgs = create_dict_given_info(args.model, args.initDate, args.freq, args.var)
		if args.srcDir:
			files = get_nc_files(args.srcDir)
			for f in files:
				pathDict = get_path_info(f)
				dump_metadata(pathDict, args.metadataFolder)
 				log(logFile, os.path.basename(f), "", 'File Started')
				if fix_filename(pathDict, logFile, args.fixFlag, args.histFlag):
					log(logFile, os.path.basename(f), "", "File Confirmed")

		elif args.fileName:
			pathDict = get_path_info(args.fileName)
			dump_metadata(pathDict, args.metadataFolder)
			log(logFile, os.path.basename(args.fileName), "", 'File Started')
			if fix_filename(pathDict, logFile, args.fixFlag, args.histFlag):
				log(logFile, os.path.basename(args.fileName), "", "File Confirmed")

if __name__ == "__main__":
	main()

