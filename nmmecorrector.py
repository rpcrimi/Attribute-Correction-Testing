import pymongo
from difflib import SequenceMatcher
import logging
import os
import shutil
import argparse
import datetime
import subprocess
import shlex
import sys
import re
from progressbar import *
import dropDB
import mongoinit
import pprint


connection        = pymongo.MongoClient()
db                = connection["Attribute_Correction"]
CFVars            = db["CFVars"]
StandardNameFixes = db["StandardNameFixes"]
VarNameFixes      = db["VarNameFixes"]
FreqFixes         = db["FreqFixes"]
realizationRegex  = re.compile('r[0-9]+i[0-9]+p[0-9]+')

# Class to handle logging calls
class Logger:
	def __init__(self, logFile=None):
		self.logFile = logFile

	# Return formated date/time for logfile
	@staticmethod
	def get_datetime(): return str(datetime.datetime.now()).split(".")[0].replace(" ", "T")

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
			logging.info("Starting in file: [%s]", fileName)

		elif logType == 'File Confirmed':
			logging.info("Confirmed file: [%s]", fileName)

		# Standard Name/Units Logs
		elif logType == 'Standard Name Confirmed':
			logging.info("Standard Name [%s] confirmed", text)

		elif logType == 'Switched Standard Name':
			logging.debug("Switched [%s] standard_name from [%s] to [%s]", text[0], text[1], text[2])

		elif logType == 'Switched Variable':
			logging.debug("Switched variable name from [%s:%s] to [%s:%s]", text[0], text[2], text[1], text[2])

		elif logType == 'Estimated Standard Name':
			logging.debug("Standard Name [%s:%s] best 3 estimates: %s", text[0], text[1], text[2])

		elif logType == 'No Standard Names':
			logging.debug("[%s]: no standard names defined", fileName)

		elif logType == 'No Matching Var Name':
			logging.debug("[%s] recommended Variable Names: %s", text[0], text[1])

		elif logType == 'Changed Units':
			logging.debug("Changed [%s] units from [%s] to [%s]", text[0], text[1], text[2])

		elif logType == 'Confirmed Units':
			logging.info("Units [%s:%s] confirmed", text[0], text[1])


		# File Name Logs
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

	# Return the location of file==fileName
	def get_file_name_location(self, fileName):
		scriptDir = os.path.dirname(os.path.abspath(__file__))
		for root, dirs, files in os.walk(scriptDir):
			if fileName in files:
				return os.path.join(root, fileName) + " "

	# Grab the attribute==attr from the file==fullPath
	# This should only be used for global attributes
	def get_metadata(self, fullPath, var, attr):
		# Create the grep string
		if var:
			grep = 'grep ' + var+":"+attr
		else:
			grep = 'grep :'+attr
		dump = './ncdump.sh ' + fullPath
		# Dump metadata and grep for attribute
		p  = subprocess.Popen(shlex.split(dump), stdout=subprocess.PIPE)
		p2 = subprocess.Popen(shlex.split(grep), stdin=p.stdout, stdout=subprocess.PIPE)
		p.stdout.close()
		out, err = p2.communicate()
		p2.stdout.close()
		# Format metadata by removing tabs, newlines, and semicolons and grabbing the value
		if out:
			metadata = out.replace("\t", "").replace("\n", "").replace(" ;", "").split(" = ")[1].strip('"')
			return metadata
		else:
			return "No Metadata"

	# Grab the header from file==fullPath
	def ncdump(self, fullPath):
		call = self.get_file_name_location("ncdump.sh") + fullPath 
		p = subprocess.Popen(shlex.split(call), stdout=subprocess.PIPE)
		out, err = p.communicate()
		p.stdout.close()
		if err: print(err)
		else: return out

	# Edit the attribute in the metadata of file==inputFile
	# histFlag==True means do not update history
	def ncatted(self, att_nm, var_nm, mode, att_type, att_val, inputFile, histFlag, outputFile=""):
		call = self.get_file_name_location("ncatted.sh") + "%s %s %s %s %s %s %s %s" % (att_nm, var_nm, mode, att_type, att_val, inputFile, outputFile, ("" if histFlag else "-h"))
		p = subprocess.Popen(shlex.split(call))
		out, err = p.communicate()
		if err: print(err)

	# Rename the variable from oldName to newName in file==inputFile
	def ncrename(self, oldName, newName, inputFile, histFlag, outputFile=""):
		call = self.get_file_name_location("ncrename.sh") + "%s %s %s %s %s" % (oldName, newName, inputFile, outputFile, ("" if histFlag else "-h"))
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

		fileName = dstDir+pathDict["fileName"].replace(pathDict["extension"], ".txt")
		with open(fileName, "w") as text_file:
			text_file.write(out)

	# Return formated output of grep call
	def format_output(self, out):
		# Remove tabs and newlines and split on " ;"
		out = out.replace("\t", "").replace("\n", "").replace("standard_name = ", "").replace("\"", "").split(" ;")
		# Return list without empty elements
		return filter(None, out)

	# Return a list of all netCDF files in "direrctory"
	def get_nc_files(self, directory, dstFolder):
		matches = []
		# Do a walk through input directory
		for root, dirnames, files in os.walk(directory):
			# Find all filenames with .nc type
			for filename in files:
					if filename.endswith(('.nc', '.nc4')):
						filename = os.path.join(root, filename)
						dstFileName = dstFolder + filename
						if not os.path.isfile(dstFileName):
							# Add full path of netCDF file to matches list
							matches.append(filename)
		return matches

	# Return a list of filenames and corresponding standard names in "ncFolder"
	def get_standard_names_units(self, ncFolder, dstFolder):
		standardNames = []
		# Call ncdump and grep for :standard_name for each netCDF file in ncFolder
		for f in self.get_nc_files(ncFolder, dstFolder):
			p  = subprocess.Popen(['./ncdump.sh', f], stdout=subprocess.PIPE)
			p2 = subprocess.Popen(shlex.split('grep :standard_name'), stdin=p.stdout, stdout=subprocess.PIPE)
			p.stdout.close()
			out, err = p2.communicate()
			standardNames.append((f, self.format_output(out)))
			p2.stdout.close()
		
		return standardNames

# Class to validate file names and metadata
class FileNameValidator:
	def __init__(self, srcDir, fileName, metadataFolder, logger, fixFlag, histFlag):
		if srcDir: 
			self.srcDir   = srcDir
			self.fileName = None
		else:
			self.srcDir   = None      
			self.fileName = fileName
		self.metadataController  = MetadataController(metadataFolder)
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
		if 'NOAA-GFDL' in fullPath:
			institute_id_index              = splitFileName.index('NOAA-GFDL')
			dictionary["institute_id"]      = splitFileName[institute_id_index]
			dictionary["model_id"]          = splitFileName[institute_id_index+1]
			dictionary["experiment_id"]     = splitFileName[institute_id_index+2]
			dictionary["frequency"]         = splitFileName[institute_id_index+3]
			dictionary["modeling_realm"]    = splitFileName[institute_id_index+4]
			dictionary["variable"]          = splitFileName[institute_id_index+6]
		elif 'CCCMA' in fullPath:
			institute_id_index              = splitFileName.index('CCCMA')
			dictionary["institute_id"]      = splitFileName[institute_id_index]
			dictionary["model_id"]          = splitFileName[institute_id_index+1]
			dictionary["experiment_id"]     = splitFileName[institute_id_index+2]
			dictionary["frequency"]         = splitFileName[institute_id_index+3]
			dictionary["modeling_realm"]    = splitFileName[institute_id_index+4]
			dictionary["variable"]          = splitFileName[institute_id_index+6]
		elif 'UM-RSMAS' in fullPath:
			institute_id_index              = splitFileName.index('UM-RSMAS')
			dictionary["institute_id"]      = splitFileName[institute_id_index]
			dictionary["model_id"]          = splitFileName[institute_id_index+1]
			dictionary["experiment_id"]     = splitFileName[institute_id_index+2]
			dictionary["frequency"]         = splitFileName[institute_id_index+3]
			dictionary["modeling_realm"]    = splitFileName[institute_id_index+4]
			dictionary["variable"]          = splitFileName[institute_id_index+5]
		elif 'NASA-GMAO' in fullPath:
			institute_id_index              = splitFileName.index('NASA-GMAO')
			dictionary["institute_id"]      = splitFileName[institute_id_index]
			dictionary["model_id"]          = splitFileName[institute_id_index+1]
			dictionary["experiment_id"]     = splitFileName[institute_id_index+2]
			dictionary["frequency"]         = splitFileName[institute_id_index+3]
			dictionary["modeling_realm"]    = splitFileName[institute_id_index+4]
			dictionary["variable"]          = splitFileName[institute_id_index+5]
		
		dictionary["project_id"]            = "NMME"
		dictionary["startyear"]             = dictionary["experiment_id"][:4]
		dictionary["startmonth"]            = dictionary["experiment_id"][4:6]

		dictionary["fileName"]              = os.path.basename(fullPath)
		dictionary["dirName"]               = os.path.dirname(fullPath)
		dictionary["fullPath"]              = fullPath
		dictionary["extension"]             = "."+fullPath.split(".")[-1]
		dictionary["ensemble"]              = [match for match in fullPath.split("_") if re.match(realizationRegex, match)][0].replace(dictionary["extension"], "")
		dictionary["realization"]           = dictionary["ensemble"].replace("r", "").split("i")[0]

		dictionary["rootFileName"]          = ".".join(fullPath.split(".")[:-1])
		if not re.match(realizationRegex, dictionary["rootFileName"].split("_")[-1]):
			dictionary["endDate"]           = dictionary["rootFileName"].split("-")[-1]
			dictionary["endyear"]           = dictionary["endDate"][:4]
			dictionary["endmonth"]          = dictionary["endDate"][4:6]
			dictionary["startEnd"]          = "_"+dictionary["experiment_id"] + "-" + dictionary["endDate"]
		else:
			dictionary["startEnd"]          = ""

		self.pathDicts[fullPath] = dictionary

	# Return new file name based on path information
	def get_new_filename(self, pathDict):
		return pathDict["variable"]+"_"+pathDict["frequency"]+"_"+pathDict["model_id"]+"_"+pathDict["experiment_id"]+"_"+pathDict["ensemble"]+pathDict["startEnd"]+pathDict["extension"]

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
		for meta in ["frequency", "realization", "model_id", "modeling_realm", "institute_id", "startyear", "startmonth", "endyear", "endmonth", "experiment_id", "project_id"]:
			metadata = self.metadataController.get_metadata(pathDict["fullPath"], None ,meta)
			if meta in pathDict and metadata != pathDict[meta]:
				if self.fixFlag:
					# Update the metadata to path information
					self.metadataController.ncatted(meta, "global", "o", "c", pathDict[meta], pathDict["fullPath"], ("-h" if self.histFlag else ""))
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
			self.metadataController.dump_metadata(self.pathDicts[f])
 			self.logger.log(self.pathDicts[f]["fullPath"], "", 'File Started')
 			# fix_filename saw no errors ==> file is confirmed
			if self.fix_filename(f):
				self.logger.log(self.pathDicts[f]["fullPath"], "", "File Confirmed")
			bar.update(i)
			i = i + 1
		bar.finish()	

class StandardNameValidator:
	def __init__(self, srcDir, fileName, dstDir, metadataFolder, logger, fixFlag, histFlag):
		if srcDir: 
			self.srcDir          = srcDir
			self.fileName        = None
		else:
			self.srcDir          = None      
			self.fileName        = fileName
		self.dstDir              = dstDir
		self.metadataController  = MetadataController(metadataFolder)
		self.logger              = logger
		self.fileFlag            = True
		self.fixFlag             = fixFlag
		self.histFlag            = histFlag
		self.pathDicts           = {}

	# Save all path info to pathDicts[fullPath] entry
	def get_path_info(self, fullPath):
		dictionary = {}
		splitFileName = fullPath.split("/")
		if 'NOAA-GFDL' in fullPath:
			institute_id_index              = splitFileName.index('NOAA-GFDL')
			dictionary["institute_id"]      = splitFileName[institute_id_index]
			dictionary["model_id"]          = splitFileName[institute_id_index+1]
			dictionary["experiment_id"]     = splitFileName[institute_id_index+2]
			dictionary["frequency"]         = splitFileName[institute_id_index+3]
			dictionary["modeling_realm"]    = splitFileName[institute_id_index+5]
			dictionary["variable"]          = splitFileName[institute_id_index+6]
		elif 'CCCMA' in fullPath:
			institute_id_index              = splitFileName.index('CCCMA')
			dictionary["institute_id"]      = splitFileName[institute_id_index]
			dictionary["model_id"]          = splitFileName[institute_id_index+1]
			dictionary["experiment_id"]     = splitFileName[institute_id_index+2]
			dictionary["frequency"]         = splitFileName[institute_id_index+3]
			dictionary["modeling_realm"]    = splitFileName[institute_id_index+4]
			dictionary["variable"]          = splitFileName[institute_id_index+6]
		elif 'UM-RSMAS' in fullPath:
			institute_id_index              = splitFileName.index('UM-RSMAS')
			dictionary["institute_id"]      = splitFileName[institute_id_index]
			dictionary["model_id"]          = splitFileName[institute_id_index+1]
			dictionary["experiment_id"]     = splitFileName[institute_id_index+2]
			dictionary["frequency"]         = splitFileName[institute_id_index+3]
			dictionary["modeling_realm"]    = splitFileName[institute_id_index+4]
			dictionary["variable"]          = splitFileName[institute_id_index+5]
		elif 'NASA-GMAO' in fullPath:
			institute_id_index              = splitFileName.index('NASA-GMAO')
			dictionary["institute_id"]      = splitFileName[institute_id_index]
			dictionary["model_id"]          = splitFileName[institute_id_index+1]
			dictionary["experiment_id"]     = splitFileName[institute_id_index+2]
			dictionary["frequency"]         = splitFileName[institute_id_index+3]
			dictionary["modeling_realm"]    = splitFileName[institute_id_index+4]
			dictionary["variable"]          = splitFileName[institute_id_index+5]
		
		dictionary["project_id"]            = "NMME"
		dictionary["startyear"]             = dictionary["experiment_id"][:4]
		dictionary["startmonth"]            = dictionary["experiment_id"][4:6]

		dictionary["fileName"]              = os.path.basename(fullPath)
		dictionary["dirName"]               = os.path.dirname(fullPath)
		dictionary["fullPath"]              = fullPath
		dictionary["extension"]             = "."+fullPath.split(".")[-1]
		dictionary["ensemble"]              = [match for match in fullPath.split("_") if re.match(realizationRegex, match)][0].replace(dictionary["extension"], "")
		dictionary["realization"]           = dictionary["ensemble"].replace("r", "").split("i")[0]

		dictionary["rootFileName"]          = ".".join(fullPath.split(".")[:-1])
		if not re.match(realizationRegex, dictionary["rootFileName"].split("_")[-1]):
			dictionary["endDate"]           = dictionary["rootFileName"][-8:]
			dictionary["endyear"]           = dictionary["endDate"][:4]
			dictionary["endmonth"]          = dictionary["endDate"][4:6]
			dictionary["startEnd"]          = "_"+dictionary["experiment_id"] + "-" + dictionary["endDate"]
		else:
			dictionary["startEnd"]          = ""

		self.pathDicts[fullPath] = dictionary

	# Return list of CF Standard Names from CFVars Collection
	def get_CF_Standard_Names(self):
		# Query CFVars for all Variables
		cursor = db.CFVars.find()
		CFStandards = []
		# Append each CF STandard Name to CFStandards list
		for standardName in cursor:
			CFStandards.append(standardName["CF Standard Name"])
		return CFStandards

	# Return similarity ratio of string "a" and "b"
	def similar(self, a, b):
		a = a.lower()
		b = b.lower()
		return SequenceMatcher(None, a, b).ratio()*100

	# Return the "N" # of CF Standard Vars with the most similarity to "wrongAttr"
	def best_estimates(self, wrongAttr):
		# Grab CF Standard Names
		CFStandards = self.get_CF_Standard_Names()
		similarities = []
		# Calculate percent difference between the wrong attribute and each CF Standard Name
		# Append (standardName, percentOff) tuple to similarities for future sorting
		for standardName in CFStandards:
			percentOff = self.similar(wrongAttr, standardName)
			similarities.append((standardName, percentOff))
		# Sort similarities list by second element in tuple
		similarities.sort(key=lambda x: x[1])

		return list(reversed(similarities[-3:]))

	def estimate_standard_name(self, var, standardName, fileName):
		bestEstimatesList = self.best_estimates(standardName)
		bestEstimates = ""
		for e in bestEstimatesList:
			bestEstimates += str(e[0]) + " | "
		self.logger.log(fileName, [var, standardName, bestEstimates], 'Estimated Standard Name')

	def estimate_var_name(self, var, standardName, fileName):
		cursor = db.CFVars.find({"Var Name": { '$eq': var.lower()}})
		recommendations = ""
		for v in cursor:
			recommendations += v["Var Name"] + " | "
		self.logger.log(fileName, [var+":"+standardName, recommendations], 'No Matching Var Name')	

	def validate_var_standard_name_pair(self, var, standardName, fileName):
		# Check if (var, standardName) is valid CF Standard Name pair
		cursor = db.CFVars.find_one({ '$and': [{"CF Standard Name": { '$eq': standardName}}, {"Var Name": {'$eq': var}}]})
		# Log notification of correct attribute
		if (cursor):
			# Check units for var, standardName pair
			metadataUnits = self.metadataController.get_metadata(fileName, var, "units")
			if cursor["Units"] != metadataUnits:
				if self.fixFlag:
					self.metadataController.ncatted("units", var, "o", "c", cursor["Units"], fileName, self.histFlag)
				self.logger.log(fileName, [var, metadataUnits, cursor["Units"]], 'Changed Units')
			else:
				self.logger.log(fileName, [var, metadataUnits], 'Confirmed Units')

			text = var + ":" + standardName
			self.logger.log(fileName, text, "Standard Name Confirmed")
			# Return true for confirming file
			return True

	def find_var_known_fix(self, var, standardName, fileName):
		# Check if (var, standardName) pair is in VarNameFixes collection
		cursor = db.VarNameFixes.find_one({ '$and': [{"Incorrect Var Name": { '$eq': var}}, {"CF Standard Name": {'$eq': standardName}}]})
		if (cursor):
			if self.fixFlag:
				self.metadataController.ncrename(var, cursor["Known Fix"], fileName, self.histFlag)
			# Log the fix
			self.logger.log(fileName, [var, cursor["Known Fix"], standardName], 'Switched Variable')
			return (cursor["Known Fix"], True)
		else:
			return (var, False)

	def find_standard_name_fix(self, var, standardName, fileName):
		# Set all characters to lowercase
		standardName = standardName.lower()
		# Check if KnownFixes has seen this error before
		cursor = db.StandardNameFixes.find_one({ '$and': [{"Incorrect Standard Name": { '$eq': standardName}}, {"Var Name": {'$eq': var}}]})
		# If standardName exists in StandardNameFixes collection
		if (cursor):
			if self.fixFlag:
				self.metadataController.ncatted("standard_name", var, "o", "c", cursor["Known Fix"], fileName, self.histFlag)
			# Log the fix
			self.logger.log(fileName, [var, standardName, cursor["Known Fix"]], 'Switched Standard Name')
			return (cursor["Known Fix"], True)
		else:
			return (standardName, False)
	
	def confirm_file(self, fileName):
		if self.fileFlag:
			if self.fixFlag:
				# New path for copying file
				dstdir = self.dstDir+os.path.dirname(fileName)
				# If path does not exist ==> create directory structure
				if not os.path.exists(dstdir):
					os.makedirs(dstdir)
				# Copy original file to dstdir
				shutil.move(fileName, dstdir)
			# Log the confirmed file
			self.logger.log(fileName, "", 'File Confirmed')

	# Return validation of correct attribute
	# or corrected attribute from Known fixes collection
	# or return the top 3 matches from CFVars collection
	def identify_attribute(self, var, standardName, fileName):
		# Check if (var, standardName) is valid CF Standard Name pair
		if self.validate_var_standard_name_pair(var, standardName, fileName):
			return True

		(standardName, sFlag) = self.find_standard_name_fix(var, standardName, fileName)
		if self.validate_var_standard_name_pair(var, standardName, fileName):
			return False	

		(var, vFlag) = self.find_var_known_fix(var, standardName, fileName)
		if self.validate_var_standard_name_pair(var, standardName, fileName):
			return False

		if not sFlag:
			self.estimate_standard_name(var, standardName, fileName)
		if not vFlag:
			self.estimate_var_name(var, standardName, fileName)
		return False

	def validate(self):
		# (filename, standard_name, units) list of all files in ncFolder
		standardNamesUnits = self.metadataController.get_standard_names_units(self.srcDir, self.dstDir)
		if standardNamesUnits:
			# Number of files for use in progress bar
			totalFiles = len(standardNamesUnits)
			i = 1
			widgets = ['Percent Done: ', Percentage(), ' ', AnimatedMarker(), ' ', ETA()]
			bar = ProgressBar(widgets=widgets, maxval=totalFiles).start()
			# For each file in the list
			for f in standardNamesUnits:
				fileName   = f[0]
				standNames = f[1]
				self.get_path_info(fileName)
				self.metadataController.dump_metadata(self.pathDicts[fileName])
				self.logger.log(fileName, "", 'File Started')
				# If the file has no standard names, log the issue
				if not standNames:
					self.logger.log(fileName, "", 'No Standard Names')
					self.fileFlag = False
				# For each attribute in standard_name list, format and identify attribute
				else:
					for attr in standNames:
						splitAttr = attr.split(":")
						# Check if something in file was changed
						if not self.identify_attribute(splitAttr[0], splitAttr[1], fileName):
							self.fileFlag = False
				# If file had no errors or KnownFix occured ==> Confirm file
				self.confirm_file(fileName)
				# Reset fileFlag
				self.fileFlag = True
				# Update progress bar
				bar.update(i)
				i = i + 1
			bar.finish()


def main():
	parser = argparse.ArgumentParser(description='File Name Correction Algorithm')
	parser.add_argument("-o", "--op", "--operation", dest="operation",      help = "Operation to run (initDB, resetDB, snf=standard names fix, fnf=file names fix)", default="both")
	parser.add_argument("-s", "--src", "--srcDir",   dest="srcDir",         help = "Source Directory")
	parser.add_argument("--file", "--fileName",      dest="fileName",       help = "File Name for single file fix")
	parser.add_argument("-d", "--dstDir",            dest="dstDir",         help = "Folder to copy fixed files to")
	parser.add_argument("-m", "--metadata",          dest="metadataFolder", help = "Folder to dump original metadata to")
	parser.add_argument("-l", "--logFile",           dest="logFile",        help = "File to log metadata changes to")
	parser.add_argument("--fix", "--fixFlag",        dest="fixFlag",        help = "Flag to fix file names or only report possible changes (-f = Fix File Names)",  action='store_true',  default=False)
	parser.add_argument("--hist", "--histFlag",      dest="histFlag",       help = "Flag to append changes to history metadata (-h = do not append to history)",    action='store_true',  default=False)

	args = parser.parse_args()
	if(len(sys.argv) == 1):
		parser.print_help()

	else:
		if args.operation == "initDB":
			mongoinit.run()
		elif args.operation == "resetDB":
			dropDB.run()
			mongoinit.run()
		elif args.operation == "snf":
			l = Logger(args.logFile)
			l.set_logfile(args.srcDir)
			v = StandardNameValidator(args.srcDir, args.fileName, args.dstDir, args.metadataFolder, l, args.fixFlag, args.histFlag)
			v.validate()
		elif args.operation == "fnf":
			l = Logger(args.logFile)
			l.set_logfile(args.srcDir or args.fileName)
			v = FileNameValidator(args.srcDir, args.fileName, args.metadataFolder, l, args.fixFlag, args.histFlag)
			v.validate()

if __name__ == "__main__":
	main()