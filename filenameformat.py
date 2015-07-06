import pymongo
import ntpath
import subprocess
import shlex
import re

connection        = pymongo.MongoClient()
db                = connection["Attribute_Correction"]
CFVars            = db["CFVars"]
ValidFreq         = db["ValidFreq"]
realizationRegex  = re.compile('r[0-9]+i[0-9]+p[0-9]+')

def run(fullPath):
	fileName      = ntpath.basename(fullPath)
	splitFileName = fileName.split("_")

	# Validate Variable
	var           = splitFileName[0].lower()
	cursor = db.CFVars.find_one({"Var Name": var})
	if not cursor:
		print "Invalid Var Name"

	# Validate Frequenct
	freq          = splitFileName[1]
	cursor = db.ValidFreq.find_one({"Frequency": freq})
	if not cursor:
		print "Invalid Frequency"

	# Validate realization number
	realization   = [match for match in splitFileName if re.match(realizationRegex, match)][0]
	p  = subprocess.Popen(['./ncdump.sh', fullPath], stdout=subprocess.PIPE)
	p2 = subprocess.Popen(shlex.split('grep :realization'), stdin=p.stdout, stdout=subprocess.PIPE)
	p.stdout.close()
	out, err = p2.communicate()
	p2.stdout.close()
	print(out.replace("\t", "").replace("\n", ""))

	splitFullPath = fullPath.split("/")
	model    = splitFullPath[1]
	initDate = splitFullPath[2]

	print fullPath
	print(var+"_"+freq+"_"+model+"_"+initDate+"_"+realization)



NOAA = "NOAA-GFDL/day/atmos/v20140710/pr/pr_day_GFDL-FLORB01_FLORB01-P1-ECDA-v3.1-011980_r10i1p1_19800101-19801231.nc"
CCSM4 = "UM-RSMAS/CCSM4/19820101/day/atmos/g/g_day_CCSM4_19820101_r10i1p1_19820101-19821231.nc"

run(NOAA)