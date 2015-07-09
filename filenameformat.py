import pymongo
import subprocess
import shlex
import re
import argparse
import os
import logging
import sys
import ncatted

connection        = pymongo.MongoClient()
db                = connection["Attribute_Correction"]
CFVars            = db["CFVars"]
ValidFreq         = db["ValidFreq"]
realizationRegex  = re.compile('r[0-9]+i[0-9]+p[0-9]+')
NOAA = "NOAA-GFDL/day/atmos/v20140710/pr/pr_day_GFDL-FLORB01_FLORB01-P1-ECDA-v3.1-011980_r10i1p1_19800101-19801231.nc"
CCSM4 = "UM-RSMAS/CCSM4/19820101/day/atmos/g/g_day_CCSM4_19820101_r10i1p1_19820101-19821231.nc"

# Log info in "logFile" for file "fileName"
def log(logFile, fileName, text, logType):
	logging.basicConfig(level=logging.DEBUG, format='%(levelname)-8s %(message)s', filename=logFile, filemode='w')
	if logType == 'File Started':
		logging.info("--------------------------------------------------------------------------------------------------------------------------------------")
		logging.debug("Starting file name: [%s]", fileName)

	elif logType == 'File Confirmed':
		logging.debug("Confirmed file name: [%s]", fileName)
		logging.info("--------------------------------------------------------------------------------------------------------------------------------------")

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

# Get model, initialization date, frequency, and variable from the full path of the given file
def get_model_initdate_freq_var(fullPath, srcDir):
	splitFileName = fullPath.split("/")
	if srcDir == 'NOAA-GFDL/' or srcDir == 'CCCMA/':
		return (splitFileName[1], splitFileName[2], splitFileName[3], splitFileName[6])
	elif srcDir == 'UM-RSMAS/' or srcDir == 'NASA-GMAO/':
		return (splitFileName[1], splitFileName[2], splitFileName[3], splitFileName[5])

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
def get_metadata(fullPath, attr):
	# Create the grep string
	grep = 'grep :'+attr
	# Dump metadata and grep for attribute
	p  = subprocess.Popen(['./ncdump.sh', fullPath], stdout=subprocess.PIPE)
	p2 = subprocess.Popen(shlex.split(grep), stdin=p.stdout, stdout=subprocess.PIPE)
	p.stdout.close()
	out, err = p2.communicate()
	p2.stdout.close()
	# Format metadata by removing tabs, newlines, and semicolons and grabbing the value
	# lstrip("0") for realization numbers of the form r01i1p1
	metadata = out.replace("\t", "").replace("\n", "").replace(" ;", "").split(" = ")[1].strip('"').lstrip("0")
	return metadata

# Fix the file = fullPath
# scrDir is required for pulling correct data from path
# Changes will occur if fixFlag == True
# Changes will appear in history if histFlag == False
def fix_filename(fullPath, srcDir, logFile, fixFlag, histFlag):
	flag          = True
	fileName      = os.path.basename(fullPath)
	model, initDate, freq, var = get_model_initdate_freq_var(fullPath, srcDir)
	splitFileName = fileName.split("_")

	# Validate Variable
	#------------------
	if not db.CFVars.find_one({"Var Name": var}):
		# Try to fix the variable name by making characters lowercase
		#------------------------------------------------------------
		if db.CFVars.find_one({"Var Name": var.lower()}):
			if fixFlag:
				var = var.lower()
			log(logFile, fileName, [var.upper(), var.lower()], 'Var Name Fix')

			# Fix the folder that is named after the variable
			#------------------------------------------------
			oldDir      = os.path.dirname(fullPath)
			# Get the name of the variable folder
			parDirIndex = oldDir.rfind('/')
			parDir      = oldDir[parDirIndex+1:]
			# If folder is uppercase ==> make lowercase and rename folder
			if parDir.isupper():
				parDir = parDir.lower()
				newDir = oldDir[:parDirIndex+1]+parDir
				if fixFlag:
					os.rename(oldDir, newDir)
				log(logFile, fileName, [oldDir, newDir], 'Renamed Var Folder')
		else:
			log(logFile, fileName, var, 'Var Error')
		# Error seen	
		flag = False

	# Validate Frequency
	#-------------------
	if not db.ValidFreq.find_one({"Frequency": freq}):
		log(logFile, fileName, freq, 'Freq Error')
		# Error seen
		flag = False

	# Validate realization number
	#----------------------------
	# Grab realization number from fileName
	fileNameRealization = [match for match in splitFileName if re.match(realizationRegex, match)][0].replace(".nc", "").rstrip("4")
	# Grab realization number from metadata
	realization         = get_metadata(fullPath, "realization")
	realization         = "r"+realization+"i1p1"
	# If the two values differ ==> fix the value in metadata to reflect filename
	if realization != fileNameRealization:
		log(logFile, fileName, [realization, fileNameRealization], 'Realization Error')
		if fixFlag:
			# Find the number after "r" in fileNameRealization value
			realizationNum = map(int, re.findall(r'\d+', fileNameRealization))[0]
			# Overwrite realization value in metadata
			ncatted.run("realization", "global", "o", "i", realizationNum, fullPath, ("-h" if histFlag else ""))
			log(logFile, fileName, [realization, fileNameRealization], 'Realization Fix')
		# Error seen
		flag = False

	# Create End Date and File Extension
	#-----------------------------------
	splitFileName = fileName.split(".")
	extension     = splitFileName[-1]
	rootFileName  = ".".join(splitFileName[0:-1])
	# If end of filename is not a realization number ==> it contains a start-end date
	if not re.match(realizationRegex, rootFileName.split("_")[-1]):
		# Grab the enddate from the file
		# TODO: FIX FOR NON 8 CHARACTER DATES
		endDate  = rootFileName[-8:]
		startEnd = initDate + "-" + endDate
	# Do not append start-end date if none is provided
	else:
		startEnd = ""

	# Create filename based on pulled information
	#--------------------------------------------
	newFileName = var+"_"+freq+"_"+model+"_"+initDate+"_"+fileNameRealization+("_" if startEnd else "")+startEnd+"."+extension
	# If filename differs from created filename ==> rename file to created filename
	if fileName != newFileName:
		log(logFile, fileName, [fileName, newFileName], 'File Name Error')
		if fixFlag:
			newFullPath = os.path.dirname(fullPath)+"/"+newFileName
			os.rename(fullPath, newFullPath)
			log(logFile, fileName, newFileName, 'Renamed File Name')
		# Error seen
		flag = False

	# Return boolean value if error found
	return flag

def main():
	parser = argparse.ArgumentParser(description='File Name Correction Algorithm')
	parser.add_argument("-s", "--src", "--srcDir", dest="srcDir",   help = "Source Directory")
	parser.add_argument("-f", "--fileName",        dest="fileName", help = "File Name for single file fix")
	parser.add_argument("-m", "--model",           dest="model",    help = "Name of model (ex: NOAA-GFDL, CCSM4). This argument will overwrite any found model in metadata, path, or filename")
	parser.add_argument("-i", "--initDate",        dest="initDate", help = "Initialization date (ex: 20090101). This argument will overwrite any found initDate in metadata, path, or filename")
	parser.add_argument("--freq", "--frequency",   dest="freq",     help = "Frequency (ex: day, mon, Omon). This argument will overwrite any found frequency in metadata, path, or filename")
	parser.add_argument("-v", "--var",             dest="var",      help = "Variable name (ex: pr, tasmax, hus). This argument will overwrite any found variable in metadata, path, or filename")
	parser.add_argument("-l", "--logFile",         dest="logFile",  help = "File to log metadata changes to")
	parser.add_argument("--fix", "--fixFlag",      dest="fixFlag",  help = "Flag to fix file names or only report possible changes (-f = Fix File Names)",  action='store_true', default=False)
	parser.add_argument("--hist", "--histFlag",    dest="histFlag", help = "Flag to append changes to history metadata (-h = do not append to history)",           action='store_true', default=False)

	args = parser.parse_args()
	if(len(sys.argv) == 1):
		parser.print_help()

	else:
		givenArgs = create_dict_given_info(args.model, args.initDate, args.freq, args.var)
		if args.srcDir:
			files = get_nc_files(args.srcDir)
			for f in files:
				log(args.logFile, os.path.basename(f), "", 'File Started')
				if fix_filename(f, args.srcDir, args.logFile, args.fixFlag, args.histFlag):
					log(args.logFile, os.path.basename(f), "", "File Confirmed")

		elif args.fileName:
			log(args.logFile, os.path.basename(args.fileName), "", 'File Started')
			if fix_filename(args.fileName, args.srcDir, args.logFile, args.fixFlag, args.histFlag):
				log(args.logFile, os.path.basename(args.fileName), "", "File Confirmed")


if __name__ == "__main__":
	main()
