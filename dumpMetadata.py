# Open shell process
import subprocess
# Get current working directory
import os
# Print dictionary
import pprint

currentDir = os.getcwd()
f = currentDir + '/uv300.nc'
p = subprocess.Popen(['./ncdump.sh', f], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
out, err = p.communicate()
print(out)


metaData = out.splitlines()

dimStart  = metaData.index('dimensions:')
varStart  = metaData.index('variables:')
globStart = metaData.index('// global attributes:')

dimensions        = metaData[dimStart+1:varStart]
variables         = metaData[varStart+1:globStart]
global_attributes = metaData[globStart+1:]



attributes = {}

attributes["Dimensions"] = []
for word in dimensions:
	# Split word on " = ", and delete tab character of first word
	varName = word.split(" = ")[0].replace("\t", "")
	# Add word to "Dimensions" list
	attributes["Dimensions"].append(varName)

attributes["Variables"] = {}
for word in variables:
	if " = " in word:
		# Split word on ":"
		var = word.split(":")
		# Variable name is first word with deleted tabs
		varName = var[0].replace("\t", "")
		# Split the second word by " = "
		varValue = var[1].split(" = ")
		# Attribute name is first value
		attrName = varValue[0]
		# Attribute Value is second word with a deleted " ;"
		attrValue = varValue[1].replace(" ;", "")

		# If variable already has key value pair, add new attribute
		if varName in attributes["Variables"]:
			attributes["Variables"][varName][attrName] = attrValue
		# Else, create a dictionary for that variable and add new attribute
		else:
			attributes["Variables"][varName] = {}
			attributes["Variables"][varName][attrName] = attrValue


attributes["Global Attributes"] = {}
for word in global_attributes:
	temp = word.replace("\t", "")
	print(temp)
	# Split attribute by " = "
	#attr = word.split(" = ")
	#attrName = attr[0]
	#attrValue = attr[1]
	#attributes["Global Attributes"][attrName] = attrValue

pprint.pprint(attributes)
