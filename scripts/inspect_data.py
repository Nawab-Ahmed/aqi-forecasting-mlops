import sys
import os

# Add src/ to path so Python can find storage.mongo
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))

from storage.mongo import MongoHandler
from pymongo import ASCENDING

mongo = MongoHandler()

# 1️⃣ Total records
total = mongo.collection.count_documents({})
print(f"Total records in collection: {total}")

# 2️⃣ Count per city
pipeline = [
    {"$group": {"_id": "$city", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}}
]
print("\nRecords per city:")
for result in mongo.collection.aggregate(pipeline):
    print(f"{result['_id']}: {result['count']} records")

# 3️⃣ Last 5 records inserted
print("\nLast 5 records inserted:")
for doc in mongo.collection.find().sort("created_at", -1).limit(5):
    print(doc)

# 4️⃣ Duplicate check
pipeline = [
    {"$group": {"_id": {"city": "$city", "date": "$date"}, "count": {"$sum": 1}}},
    {"$match": {"count": {"$gt": 1}}}
]
duplicates = list(mongo.collection.aggregate(pipeline))
print(f"\nDuplicate city+date entries: {len(duplicates)}")
if duplicates:
    for d in duplicates:
        print(d)

# 5️⃣ AQI summary
pipeline = [
    {"$group": {
        "_id": None,
        "min_aqi": {"$min": "$aqi_avg"},
        "max_aqi": {"$max": "$aqi_avg"},
        "avg_aqi": {"$avg": "$aqi_avg"}
    }}
]
stats = list(mongo.collection.aggregate(pipeline))[0]
print(f"\nAQI summary -> Average: {stats['avg_aqi']}, Min: {stats['min_aqi']}, Max: {stats['max_aqi']}")
