import pymongo
import subprocess
import shlex
import re
import argparse
import os
import logging
import sys
import datetime
from progressbar import *
import ncatted
import ncdump
import pprint

connection        = pymongo.MongoClient()
db                = connection["Attribute_Correction"]
CFVars            = db["CFVars"]
ValidFreq         = db["ValidFreq"]
FreqFixes         = db["FreqFixes"]
realizationRegex  = re.compile('r[0-9]+i[0-9]+p[0-9]+')

class Logger:
	def __init__(self, logFile=None):
		self.logFile = logFile

	def get_datetime(self): return str(datetime.datetime.now()).split(".")[0].replace(" ", "T")

	def set_logfile(self, src): 
		if ".nc" in src:
			self.logFile = os.path.basename(src).replace(".nc", "").rstrip("4") + self.get_datetime() + ".log"
		else:
			self.logFile = src.replace("/", "") + "_" + self.get_datetime() + ".log"

	def log(self, fileName, text, logType):
		logging.basicConfig(level=logging.DEBUG, format='%(levelname)-8s %(message)s', filename=self.logFile, filemode='w')
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

class FileNameValidator:
	def __init__(self, srcDir, fileName, metadataFolder, logger, fixFlag, histFlag):
		if srcDir: 
			self.srcDir   = srcDir
			self.fileName = None
		else:
			self.srcDir   = None      
			self.fileName = fileName
		self.metadataFolder      = metadataFolder
		self.logger              = logger
		self.fixFlag             = fixFlag
		self.histFlag            = histFlag
		self.pathDicts           = {}

	def get_nc_files(self):
		if self.fileName:
			return [self.fileName]
		else:
			matches = []
			# Do a walk through input directory
			for root, dirnames, files in os.walk(self.srcDir):
				# Find all filenames with .nc type
				for filename in files:
					filename = os.path.join(root, filename)
					if filename.endswith(('.nc', '.nc4')):
							matches.append(filename)
			return matches
	# Get model, initialization date, frequency, and variable from the full path of the given file
	def get_path_info(self, fullPath):
		dictionary = {}
		splitFileName = fullPath.split("/")
		dictionary["institute_id"]        = splitFileName[0]
		if dictionary["institute_id"] == 'NOAA-GFDL':
			dictionary["model_id"]          = splitFileName[1]
			dictionary["initDate"]          = splitFileName[2]
			dictionary["frequency"]         = splitFileName[3]
			dictionary["modeling_realm"]    = splitFileName[5]
			dictionary["variable"]          = splitFileName[6]
		elif dictionary["institute_id"] == 'CCCMA':
			dictionary["model_id"]          = splitFileName[1]
			dictionary["initDate"]          = splitFileName[2]
			dictionary["frequency"]         = splitFileName[3]
			dictionary["modeling_realm"]    = splitFileName[4]
			dictionary["variable"]          = splitFileName[6]
		elif dictionary["institute_id"] == 'UM-RSMAS' or dictionary["institute_id"] == 'NASA-GMAO':
			dictionary["model_id"]          = splitFileName[1]
			dictionary["initDate"]          = splitFileName[2]
			dictionary["frequency"]         = splitFileName[3]
			dictionary["modeling_realm"]    = splitFileName[4]
			dictionary["variable"]          = splitFileName[5]
		
		dictionary["startyear"]             = dictionary["initDate"][:4]
		dictionary["startmonth"]            = dictionary["initDate"][4:6]
		dictionary["startday"]              = dictionary["initDate"][6:8]

		dictionary["fileName"]              = os.path.basename(fullPath)
		dictionary["dirName"]               = os.path.dirname(fullPath)
		dictionary["fullPath"]              = fullPath
		dictionary["splitFileName _"]       = fullPath.split("_")
		dictionary["splitFileName ."]       = fullPath.split(".")
		dictionary["extension"]             = dictionary["splitFileName ."][-1]

		dictionary["rootFileName"]          = ".".join(dictionary["splitFileName ."][0:-1])
		if not re.match(realizationRegex, dictionary["rootFileName"].split("_")[-1]):
			dictionary["endDate"]           = dictionary["rootFileName"][-8:]
			dictionary["startEnd"]          = "_"+dictionary["initDate"] + "-" + dictionary["endDate"]
		else:
			dictionary["startEnd"]          = ""

		self.pathDicts[fullPath] = dictionary

	def dump_metadata(self, fileName):
		out = ncdump.run(self.pathDicts[fileName]["fullPath"])
		dstDir = self.metadataFolder+self.pathDicts[fileName]["dirName"]
		# If path does not exist ==> create directory structure
		if not os.path.exists(dstDir):
			os.makedirs(dstDir)

		fileName = self.metadataFolder+self.pathDicts[fileName]["fullPath"].replace(".nc", "").rstrip("4")+".txt"
		with open(fileName, "w") as text_file:
			text_file.write(out)

	# Grab the attribute = attr from the file = fullPath
	# This should only be used for global attributes
	def get_metadata(self, pathDict, attr):
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
		if out:
			metadata = out.replace("\t", "").replace("\n", "").replace(" ;", "").split(" = ")[1].strip('"').lstrip("0")
			return metadata
		else:
			return "No Metadata"
			
	def get_new_filename(self, pathDict):
		return pathDict["variable"]+"_"+pathDict["frequency"]+"_"+pathDict["model_id"]+"_"+pathDict["initDate"]+"_"+pathDict["fileNameRealization"]+pathDict["startEnd"]+"."+pathDict["extension"]

	def validate_variable(self, fileName):
		pathDict = self.pathDicts[fileName]
		if not db.CFVars.find_one({"Var Name": pathDict["variable"]}):
			# Try to fix the variable name by making characters lowercase
			#------------------------------------------------------------
			cursor = db.VarNameFixes.find_one({"Incorrect Var Name": pathDict["variable"]})
			if cursor:
				self.logger.log(pathDict["fileName"], [pathDict["variable"], cursor["Known Fix"]], 'Var Name Fix')
				pathDict["variable"] = cursor["Known Fix"]

				# Fix the folder that is named after the variable
				#------------------------------------------------
				oldDir      = pathDict["dirName"]
				# Get the name of the variable folder
				parDirIndex = oldDir.rfind('/')
				parDir      = oldDir[parDirIndex+1:]
				# If folder is uppercase ==> make lowercase and rename folder
				parDir = cursor["Known Fix"]
				newDir = oldDir[:parDirIndex+1]+parDir
				if self.fixFlag:
					os.rename(oldDir, newDir)
				self.logger.log(pathDict["fileName"], [oldDir, newDir], 'Renamed Var Folder')
			else:
				self.logger.log(pathDict["fileName"], pathDict["variable"], 'Var Error')
			# Error seen	
			return False
		else:
			return True

	def validate_frequency(self, fileName):
		pathDict = self.pathDicts[fileName]
		if not db.ValidFreq.find_one({"Frequency": pathDict["frequency"]}):
			
			cursor = db.FreqFixes.find_one({"Incorrect Freq": pathDict["frequency"]})
			if cursor:
				# Rename Frequency folder
				oldDir = pathDict["fullPath"].split(pathDict["frequency"])[0]+pathDict["frequency"]+"/"
				newDir = pathDict["fullPath"].split(pathDict["frequency"])[0]+cursor["Known Fix"]+"/"
				if self.fixFlag:
					os.rename(oldDir, newDir)
				self.logger.log(pathDict["fileName"], [pathDict["frequency"], cursor["Known Fix"]], 'Renamed Freq Folder')
				pathDict["fullPath"]  = pathDict["fullPath"].replace(pathDict["frequency"], cursor["Known Fix"])
				pathDict["frequency"] = cursor["Known Fix"]

			else:
				log(logFile, pathDict["fileName"], pathDict["frequency"], 'Freq Error')
			# Error seen
			return False
		else:
			return True

	def validate_realization(self, fileName):
		self.pathDicts[fileName]["fileNameRealization"] = [match for match in self.pathDicts[fileName]["splitFileName _"] if re.match(realizationRegex, match)][0].replace(".nc", "").rstrip("4")
		pathDict = self.pathDicts[fileName]
		# Grab realization number from metadata
		realization = "r"+self.get_metadata(pathDict, "realization")+"i1p1"
		# If the two values differ ==> fix the value in metadata to reflect filename
		if realization != pathDict["fileNameRealization"]:
			self.logger.log(pathDict["fileName"], [realization, pathDict["fileNameRealization"]], 'Realization Error')
			if self.fixFlag:
				# Find the number after "r" in fileNameRealization value
				realizationNum = map(int, re.findall(r'\d+', pathDict["fileNameRealization"]))[0]
				# Overwrite realization value in metadata
				ncatted.run("realization", "global", "o", "i", realizationNum, pathDict["fullPath"], ("-h" if self.histFlag else ""))
				self.logger.log(pathDict["fileName"], [realization, pathDict["fileNameRealization"]], 'Realization Fix')
			# Error seen
			return False
		else:
			return True

	def validate_metadata(self, fileName):
		pathDict = self.pathDicts[fileName]
		flag = True
		for meta in ["frequency", "model_id", "modeling_realm", "institute_id", "startyear", "startmonth", "startday"]:
			metadata = self.get_metadata(pathDict, meta)
			if metadata != pathDict[meta]:
				if self.fixFlag:
					ncatted.run(meta, "global", "o", "c", pathDict[meta], pathDict["fullPath"], ("-h" if self.histFlag else ""))
				self.logger.log(pathDict["fileName"], [meta, metadata, pathDict[meta]], 'Metadata Fix')
				flag = False
		return flag

	def validate_filename(self, fileName):
		pathDict    = self.pathDicts[fileName]
		newFileName = self.get_new_filename(pathDict)
		# If filename differs from created filename ==> rename file to created filename
		if pathDict["fileName"] != newFileName:
			self.logger.log(pathDict["fileName"], [pathDict["fileName"], newFileName], 'File Name Error')
			if self.fixFlag:
				newFullPath = pathDict["dirName"]+"/"+newFileName
				os.rename(pathDict["fullPath"], newFullPath)
				self.logger.log(pathDict["fileName"], newFileName, 'Renamed File Name')
			# Error seen
			return False
		else:
			return True

	def fix_filename(self, fileName):
		varFlag         = self.validate_variable(fileName)
		freqFlag        = self.validate_frequency(fileName)
		realizationFlag = self.validate_realization(fileName)
		metadataFlag    = self.validate_metadata(fileName)
		fileFlag        = self.validate_filename(fileName)
		if not (varFlag and freqFlag and realizationFlag and metadataFlag and fileFlag):
			return False
		else:
			return True

	def validate(self):
		files = self.get_nc_files()
		totalFiles = len(files)
		i = 1
		widgets = ['Percent Done: ', Percentage(), ' ', AnimatedMarker(), ' ', ETA()]
		bar = ProgressBar(widgets=widgets, maxval=totalFiles).start()

		for f in files:
			self.get_path_info(f)
			self.dump_metadata(f)
 			self.logger.log(self.pathDicts[f]["fullPath"], "", 'File Started')
			if self.fix_filename(f):
				self.logger.log(self.pathDicts[f]["fullPath"], "", "File Confirmed")
			bar.update(i)
			i = i + 1
		bar.finish()	

def main():
	parser = argparse.ArgumentParser(description='File Name Correction Algorithm')
	parser.add_argument("-s", "--src", "--srcDir", dest="srcDir",         help = "Source Directory")
	parser.add_argument("-f", "--fileName",        dest="fileName",       help = "File Name for single file fix")
	parser.add_argument("--metadata",              dest="metadataFolder", help = "Folder to dump original metadata to", required = True)
	parser.add_argument("-l", "--logFile",         dest="logFile",        help = "File to log metadata changes to")
	parser.add_argument("--fix", "--fixFlag",      dest="fixFlag",        help = "Flag to fix file names or only report possible changes (-f = Fix File Names)",  action='store_true',  default=False)
	parser.add_argument("--hist", "--histFlag",    dest="histFlag",       help = "Flag to append changes to history metadata (-h = do not append to history)",    action='store_false', default=True)

	args = parser.parse_args()
	if(len(sys.argv) == 1):
		parser.print_help()

	else:
		l = Logger(args.logFile)
		if not args.logFile:
			l.set_logfile(args.srcDir or args.fileName)
		v = FileNameValidator(args.srcDir, args.fileName, args.metadataFolder, l, args.fixFlag, args.histFlag)
		v.validate()

if __name__ == "__main__":
	main()
