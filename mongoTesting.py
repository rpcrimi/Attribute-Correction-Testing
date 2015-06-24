import pymongo
from bson.objectid import ObjectId
import attributecorrector
from difflib import SequenceMatcher

connection = pymongo.MongoClient()
db = connection["Attribute_Correction"]
CFVars = db["CFVars"]
KnownFixes = db["KnownFixes"]

def get_CF_Standard_Names():
	cursor = db.CFVars.find()
	CFStandards = []
	for attr in cursor:
		CFStandards.append(attr["CF Standard Name"])
	return CFStandards

def similar(a,b):
	a = a.lower()
	b = b.lower()
	return SequenceMatcher(None, a, b).ratio()

def fix_attribute(wrongAttr):
	CFStandards = get_CF_Standard_Names()
	Similarities = []
	for attr in CFStandards:
		percentOff = similar(wrongAttr, attr)
		Similarities.append((attr, percentOff))
	Similarities.sort(key=lambda x: x[1])
	return Similarities


def identify_attribute(attr):
	# Check if attr is valid CF Standard Name
	cursor = db.CFVars.find({"CF Standard Name": { '$exists': True, '$eq': attr}})
	
	# If attr exists in CF Standards ==> return notification of correct attribute
	if (cursor.count() != 0):
		return "Correct Attribute"

	# attr does not exist in CF Standards
	else:
		# Set all characters to lowercase
		attr = attr.lower()
		# Check if KnownFixes has seen this error before
		cursor = db.KnownFixes.find({"Incorrect Var": { '$exists': True, '$eq': attr}})
		# If attr exists in KnownFixes
		if (cursor.count() == 1):
			# Grab id and times seen of known fix document
			_id = cursor[0]["_id"]
			timesSeen = cursor[0]["Times Seen"]

			# Update the times seen value by adding 1
			db.KnownFixes.update({"_id": _id}, {"$set": {"Times Seen": timesSeen + 1}})

			# Return the known fix for the incorrect attr
			return cursor[0]["Known Fix"]
		else:
			return fix_attribute(attr)

	return
                 
print identify_attribute("upward velocity")

