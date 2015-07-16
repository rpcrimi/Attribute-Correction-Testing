import pymongo
from difflib import SequenceMatcher
import logging
import os
import shutil
import argparse
import datetime
import subprocess
import shlex
from progressbar import *
import grabmetadata
import ncatted
import ncrename
import ncdump
import dropDB
import mongoinit
import updateCollection

connection        = pymongo.MongoClient()
db                = connection["Attribute_Correction"]
CFVars            = db["CFVars"]
StandardNameFixes = db["StandardNameFixes"]
VarNameFixes      = db["VarNameFixes"]

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
			logging.debug("Starting in file: [%s]", fileName)

		elif logType == 'File Confirmed':
			logging.info("Confirmed file: [%s]", fileName)

		elif logType == 'Standard Name Confirmed':
			logging.info("Standard Name [%s] confirmed", text)

		elif logType == 'Switched Standard Name':
			logging.info("Switched [%s] standard_name from [%s] to [%s]", text[0], text[1], text[2])

		elif logType == 'Switched Variable':
			logging.info("Switched variable name from [%s:%s] to [%s:%s]", text[0], text[2], text[1], text[2])

		elif logType == 'Estimated Standard Name':
			logging.debug("Standard Name [%s:%s] best 3 estimates: %s", text[0], text[1], text[2])

		elif logType == 'No Standard Names':
			logging.debug("[%s]: no standard names defined", fileName)

		elif logType == 'No Matching Var Name':
			logging.debug("[%s] recommended Variable Names: %s", text[0], text[1])

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
	def similar(self, a,b):
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
				self.metadataController.ncatted("standard_name", var, "o", "c", cursor["Known Fix"], fileName, histFlag)
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
				pathDict   = {}
				pathDict["fullPath"] = fileName
				pathDict["dirName"]  = os.path.dirname(fileName) 
				self.metadataController.dump_metadata(pathDict)
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
	parser = argparse.ArgumentParser(description='Metadata Correction Algorithm')
	parser.add_argument("-o", "--op", "--operation", dest="operation",      help = "Operation to run (initDB, resetDB, updateCollection, fixFiles)", default="fixFiles")
	parser.add_argument("-c", "--collection",        dest="collection",     help = "Collection to update")
	parser.add_argument("-u", "--updates",           dest="updates",        help = "JSON file containing updates")
	parser.add_argument("-s", "--srcDir",            dest="srcDir",         help = "Folder of nc or nc4 files to handle")
	parser.add_argument("-d", "--dstDir",            dest="dstDir",         help = "Folder to copy fixed files to")
	parser.add_argument("-m", "--metadata",          dest="metadataFolder", help = "Folder to dump original metadata to")
	parser.add_argument("-l", "--logFile",           dest="logFile",        help = "File to log metadata changes to")
	parser.add_argument("-f", "--fixFlag",           dest="fixFlag",        help = "Flag to fix files or only report possible changes (-f = Fix Files)",  action='store_true', default=False)
	parser.add_argument("--hist", "--histFlag",      dest="histFlag",       help = "Flag to append changes to history metadata (-h = do not append to history)", action='store_false', default=True)

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
			l = Logger(args.logFile)
			l.set_logfile(args.srcDir)

			v = StandardNameValidator(args.srcDir, None, args.dstDir, args.metadataFolder, l, args.fixFlag, args.histFlag)
			v.validate()
			return
			if (args.srcDir and args.dstDir and args.metadataFolder):
				logFile = args.logFile if args.logFile else get_logfile(args.srcDir)
				fix_files(args.srcDir, args.dstDir, logFile, args.metadataFolder, args.fixFlag, ("-h" if args.histFlag else ""))
			else:
				parser.error("fixFiles requires srcDirectory, dstDirectory, and metadataFolder")

if __name__ == "__main__":
	main()