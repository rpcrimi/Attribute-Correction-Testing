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
import pprint

connection        = pymongo.MongoClient()
db                = connection["Attribute_Correction"]
CFVars            = db["CFVars"]
ValidFreq         = db["ValidFreq"]
FreqFixes         = db["FreqFixes"]
realizationRegex  = re.compile('r[0-9]+i[0-9]+p[0-9]+')

# Class to handle logging calls
class Logger:
	def __init__(self, logFile=None):
		self.logFile = logFile

	# Return formated date/time for logfile
	def get_datetime(self): return str(datetime.datetime.now()).split(".")[0].replace(" ", "T")

	# Set logfile based on srcDir or fileName if logFile not provided
	def set_logfile(self, src):
		if self.logFile == None: 
			if ".nc" in src:
				self.logFile = os.path.basename(src).replace(".nc", "").rstrip("4") + self.get_datetime() + ".log"
			else:
				self.logFile = src.replace("/", "") + "_" + self.get_datetime() + ".log"

	# Log info of type==logType about changes to fileName
	# Text is a list of info to log
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

# Class to handle Metadata queries and changes
class MetadataController:
	def __init__(self, metadataFolder):
		self.metadataFolder = metadataFolder

	# Grab the attribute==attr from the file==fullPath
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

	# Grab the header from file==fullPath
	def ncdump(self, fullPath):
		call = "./ncdump.sh %s" % (fullPath)
		p = subprocess.Popen(shlex.split(call), stdout=subprocess.PIPE)
		out, err = p.communicate()
		p.stdout.close()
		if err: print(err)
		else: return out

	# Edit the attribute in the metadata of file==inputFile
	# histFlag==True means do not update history
	def ncatted(self, att_nm, var_nm, mode, att_type, att_val, inputFile, histFlag, outputFile=""):
		call = "./ncatted.sh %s %s %s %s %s %s %s %s" % (att_nm, var_nm, mode, att_type, att_val, inputFile, outputFile, ("-h" if histFlag else ""))
		p = subprocess.Popen(shlex.split(call))
		out, err = p.communicate()
		if err: print(err)

	# Rename the variable from oldName to newName in file==inputFile
	def ncrename(self, oldName, newName, inputFile, histFlag, outputFile=""):
		call = "./ncrename.sh %s %s %s %s %s" % (oldName, newName, ("-h" if histFlag else ""), inputFile, outputFile)
		p = subprocess.Popen(shlex.split(call))
		out, err = p.communicate()
		if err: print err

	# Dump the header of file in pathDict to same directory structure under defined metadataFolder
	def dump_metadata(self, pathDict):
		out = self.ncdump(pathDict["fullPath"])
		dstDir = self.metadataFolder+pathDict["dirName"]
		# If path does not exist ==> create directory structure
		if not os.path.exists(dstDir):
			os.makedirs(dstDir)

		fileName = self.metadataFolder+pathDict["fullPath"].replace(".nc", "").rstrip("4")+".txt"
		with open(fileName, "w") as text_file:
			text_file.write(out)

# Class to validate file names and metadata
class FileNameValidator:
	def __init__(self, srcDir, fileName, metadataFolder, logger, fixFlag, histFlag):
		if srcDir: 
			self.srcDir   = srcDir
			self.fileName = None
		else:
			self.srcDir   = None      
			self.fileName = fileName
		self.metadatacontroller  = MetadataController(metadataFolder)
		self.logger              = logger
		self.fixFlag             = fixFlag
		self.histFlag            = histFlag
		self.pathDicts           = {}

	# Return a list of all netCDF files in srcDir or just list of single fileName
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

	# Save all path info to pathDicts[fullPath] entry
	def get_path_info(self, fullPath):
		dictionary = {}
		splitFileName = fullPath.split("/")
		dictionary["institute_id"]        = splitFileName[0]
		if dictionary["institute_id"] == 'NOAA-GFDL':
			dictionary["model_id"]          = splitFileName[1]
			dictionary["experiment_id"]     = splitFileName[2]
			dictionary["frequency"]         = splitFileName[3]
			dictionary["modeling_realm"]    = splitFileName[5]
			dictionary["variable"]          = splitFileName[6]
		elif dictionary["institute_id"] == 'CCCMA':
			dictionary["model_id"]          = splitFileName[1]
			dictionary["experiment_id"]     = splitFileName[2]
			dictionary["frequency"]         = splitFileName[3]
			dictionary["modeling_realm"]    = splitFileName[4]
			dictionary["variable"]          = splitFileName[6]
		elif dictionary["institute_id"] == 'UM-RSMAS' or dictionary["institute_id"] == 'NASA-GMAO':
			dictionary["model_id"]          = splitFileName[1]
			dictionary["experiment_id"]     = splitFileName[2]
			dictionary["frequency"]         = splitFileName[3]
			dictionary["modeling_realm"]    = splitFileName[4]
			dictionary["variable"]          = splitFileName[5]
		
		dictionary["project_id"]            = "NMME"
		dictionary["startyear"]             = dictionary["experiment_id"][:4]
		dictionary["startmonth"]            = dictionary["experiment_id"][4:6]
		dictionary["startday"]              = dictionary["experiment_id"][6:8]

		dictionary["fileName"]              = os.path.basename(fullPath)
		dictionary["dirName"]               = os.path.dirname(fullPath)
		dictionary["fullPath"]              = fullPath
		dictionary["ensemble"]              = [match for match in fullPath.split("_") if re.match(realizationRegex, match)][0].replace(".nc", "").rstrip("4")
		dictionary["realization"]           = dictionary["ensemble"].replace("r", "").split("i")[0]
		dictionary["extension"]             = fullPath.split(".")[-1]

		dictionary["rootFileName"]          = ".".join(fullPath.split(".")[:-1])
		if not re.match(realizationRegex, dictionary["rootFileName"].split("_")[-1]):
			dictionary["endDate"]           = dictionary["rootFileName"][-8:]
			dictionary["startEnd"]          = "_"+dictionary["experiment_id"] + "-" + dictionary["endDate"]
		else:
			dictionary["startEnd"]          = ""

		self.pathDicts[fullPath] = dictionary

	# Return new file name based on path information
	def get_new_filename(self, pathDict):
		return pathDict["variable"]+"_"+pathDict["frequency"]+"_"+pathDict["model_id"]+"_"+pathDict["experiment_id"]+"_"+pathDict["ensemble"]+pathDict["startEnd"]+"."+pathDict["extension"]

	# Validate the variable provided in fileName
	def validate_variable(self, fileName):
		pathDict = self.pathDicts[fileName]
		# Variable does not exist in CF Standards collection
		if not db.CFVars.find_one({"Var Name": pathDict["variable"]}):
			# Try to find known fix for provided variable
			#--------------------------------------------
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
				# Change variable directory to the known fix
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

	# Validate the frequency provided in fileName
	def validate_frequency(self, fileName):
		pathDict = self.pathDicts[fileName]
		# Frequency does not exist in CF Standards collection
		if not db.ValidFreq.find_one({"Frequency": pathDict["frequency"]}):
			# Try to find known fix for provided variable
			cursor = db.FreqFixes.find_one({"Incorrect Freq": pathDict["frequency"]})
			if cursor:
				# Fix the folder that is named after the variable
				#------------------------------------------------
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

	# Validate the metadata in file==fileName
	def validate_metadata(self, fileName):
		pathDict = self.pathDicts[fileName]
		flag = True
		# For each desired value of metadata ==> Check against path information and update accordingly
		for meta in ["frequency", "realization", "model_id", "modeling_realm", "institute_id", "startyear", "startmonth", "experiment_id", "project_id"]:
			metadata = self.metadatacontroller.get_metadata(pathDict, meta)
			if metadata != pathDict[meta]:
				if self.fixFlag:
					# Update the metadata to path information
					self.metadatacontroller.ncatted(meta, "global", "o", "c", pathDict[meta], pathDict["fullPath"], ("-h" if self.histFlag else ""))
				self.logger.log(pathDict["fileName"], [meta, metadata, pathDict[meta]], 'Metadata Fix')
				flag = False
		return flag

	# Validate the provided fileName
	def validate_filename(self, fileName):
		pathDict    = self.pathDicts[fileName]
		# Create new file name based on path information
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

	# Fix the file name and metadata of file=fileName
	def fix_filename(self, fileName):
		varFlag         = self.validate_variable(fileName)
		freqFlag        = self.validate_frequency(fileName)
		metadataFlag    = self.validate_metadata(fileName)
		fileFlag        = self.validate_filename(fileName)
		if not (varFlag and freqFlag and metadataFlag and fileFlag):
			return False
		else:
			return True

	# Validate the input's file names and metadata
	def validate(self):
		# Create list of all netCDF files in input
		files = self.get_nc_files()
		totalFiles = len(files)
		i = 1
		widgets = ['Percent Done: ', Percentage(), ' ', AnimatedMarker(), ' ', ETA()]
		bar = ProgressBar(widgets=widgets, maxval=totalFiles).start()
		# Fix each file in files list
		for f in files:
			# Set pathDicts[f] entry
			self.get_path_info(f)
			# Dump metadata to same folder structure
			self.metadatacontroller.dump_metadata(self.pathDicts[f])
 			self.logger.log(self.pathDicts[f]["fullPath"], "", 'File Started')
 			# fix_filename saw no errors ==> file is confirmed
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
		l.set_logfile(args.srcDir or args.fileName)

		v = FileNameValidator(args.srcDir, args.fileName, args.metadataFolder, l, args.fixFlag, args.histFlag)
		v.validate()

if __name__ == "__main__":
	main()
