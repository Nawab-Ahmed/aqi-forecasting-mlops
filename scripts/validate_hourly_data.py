from src.storage.mongo import MongoHandler

mongo = MongoHandler()

count = mongo.collection.count_documents({})
print(f"Hourly records in MongoDB: {count}")

sample = mongo.collection.find_one()
print("Sample document:")
print(sample)
