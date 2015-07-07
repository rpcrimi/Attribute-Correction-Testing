import pymongo
import ntpath
import subprocess
import shlex
import re
import argparse
import os
import logging
import sys

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
		logging.debug("Starting in file: [%s]", fileName)

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
		splitText           = text.split(",")
		realization         = splitText[0]
		fileNameRealization = splitText[1]
		logging.debug("Metadata Realization [%s] does not match File Name Realization [%s]", realization, fileNameRealization)

	elif logType == 'File Name Error':
		splitText   = text.split(",")
		fileName    = splitText[0]
		newFileName = splitText[1]
		logging.debug("File Name [%s] does not match created File Name [%s]", fileName, newFileName)

	elif logType == 'Renamed File Name':
		logging.debug("File Name [%s] renamed to [%s]", fileName, text)

def get_model_initdate_freq_var(fullPath, model):
	splitFileName = fullPath.split("/")
	if model == 'FLORB-01':
		return (splitFileName[1], splitFileName[2], splitFileName[3], splitFileName[6])
	if model == 'CCSM4':
		return (splitFileName[1], splitFileName[2], splitFileName[3], splitFileName[5])

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

def fix_filenames(fullPath, modelName, logFile, fixFlag):
	flag          = True
	fileName      = ntpath.basename(fullPath)
	model, initDate, freq, var = get_model_initdate_freq_var(fullPath, modelName)

	splitFileName = fileName.split("_")

	# Validate Variable
	cursor = db.CFVars.find_one({"Var Name": var})
	if not cursor:
		log(logFile, fileName, var, 'Var Error')
		flag = False

	# Validate Frequency
	cursor = db.ValidFreq.find_one({"Frequency": freq})
	if not cursor:
		log(logFile, fileName, freq, 'Freq Error')
		flag = False

	# Validate Model
	if model != modelName:
		log(logFile, fileName, splitFileName[2], 'Model Error')
		flag = False

	# Validate realization number
	fileNameRealization   = [match for match in splitFileName if re.match(realizationRegex, match)][0]
	p  = subprocess.Popen(['./ncdump.sh', fullPath], stdout=subprocess.PIPE)
	p2 = subprocess.Popen(shlex.split('grep :realization'), stdin=p.stdout, stdout=subprocess.PIPE)
	p.stdout.close()
	out, err = p2.communicate()
	p2.stdout.close()
	realization = out.replace("\t", "").replace("\n", "").replace(" ;", "").split(" = ")[1].strip('"').lstrip("0")
	realization = "r"+realization+"i1p1"
	if realization != fileNameRealization:
		text = realization + "," + fileNameRealization
		log(logFile, fileName, text, 'Realization Error')
		flag = False

	# Create End Date and File Extension
	# TODO: FIX FOR NON-YEAR MODELS
	endDate   = initDate[0:4]+"1231"
	extension = fileName.split(".")[-1]

	newFileName = var+"_"+freq+"_"+model+"_"+initDate+"_"+realization+"_"+initDate+"-"+endDate+"."+extension

	if fileName != newFileName:
		text = fileName + "," + newFileName
		log(logFile, fileName, text, 'File Name Error')
		if fixFlag:
			if flag:
				newFullPath = os.path.dirname(fullPath)+"/"+newFileName
				#os.rename(fullPath, newFullPath)
				log(logFile, fileName, newFileName, 'Renamed File Name')
				flag = False
		else:
			flag = False
	return flag

def main():
	parser = argparse.ArgumentParser(description='File Name Correction Algorithm')
	parser.add_argument("-s", "--src", "--srcDir", dest="srcDir",   help = "Source Directory")
	parser.add_argument("-f", "--fileName",        dest="fileName", help = "File Name for single file fix")
	parser.add_argument("-m", "--model",           dest="model",    help = "Name of model (ex: NOAA-GFDL, CCSM4)")
	parser.add_argument("-l", "--logFile",         dest="logFile",  help = "File to log metadata changes to")
	parser.add_argument("--fix", "--fixFlag",      dest="fixFlag",  help = "Flag to fix file names or only report possible changes (-f = Fix File Names)",  action='store_true', default=False)

	args = parser.parse_args()

	if(len(sys.argv) == 1):
		parser.print_help()
	if args.srcDir:
		files = get_nc_files(args.srcDir)
		for f in files:
			log(args.logFile, ntpath.basename(f), "", 'File Started')
			flag = fix_filenames(f, args.model, args.logFile, args.fixFlag)
			if flag:
				log(args.logFile, ntpath.basename(f), "", "File Confirmed")

	elif args.fileName:
		flag = fix_filenames(args.fileName, args.model, args.logFile, args.fixFlag)
		if flag:
			log(args.logFile, args.fileName, "", "File Confirmed")


if __name__ == "__main__":
	main()