import pymongo

def run():
	c = pymongo.MongoClient()
	c.drop_database("Attribute_Correction")