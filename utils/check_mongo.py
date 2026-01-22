from utils.mongo_client import get_mongo_client

# Connect to MongoDB
client = get_mongo_client()
db = client.get_database()
collection = db["features_aqi_v1"]

# Find one document
doc = collection.find_one()
print(doc)
