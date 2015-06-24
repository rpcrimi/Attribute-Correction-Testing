import pymongo
from bson.objectid import ObjectId
import attributeCorrector

connection = pymongo.MongoClient()
db = connection["Attribute_Correction"]
CFVars = db["CFVars"]
KnownFixes = db["KnownFixes"]

def fix_attribute(attr):
	# Use attribute corrector to make guess of word
	correctedAttr = attributeCorrector.correct(attr)
	# Add corrected attribute to KnownFixes collection
	db.KnownFixes.insert({"Incorrect Var": attr, "Known Fix": correctedAttr, "Times Seen": 1})
	return correctedAttr

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

cursor = db.CFVars.find()
CFConventions = []
for x in cursor:
	CFConventions.append(x["CF Standard Name"])
                    

print identify_attribute("air-temperatur")

cursor = db.KnownFixes.find()

for x in cursor:
	print x