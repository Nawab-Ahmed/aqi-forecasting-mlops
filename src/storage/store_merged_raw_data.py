from pymongo import MongoClient
from datetime import datetime

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "aqi_project"
COLLECTION_NAME = "merged_data"

def store_merged_raw_data(df, version=None):
    if version is None:
        version = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    
    docs = [
        {
            "event_timestamp": row["event_timestamp"],
            "data": row.drop(labels=["event_timestamp"]).to_dict(),
            "version": version,
            "created_at": datetime.utcnow()
        }
        for _, row in df.iterrows()
    ]
    
    with MongoClient(MONGO_URI) as client:  # ensures proper close
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        result = collection.insert_many(docs)
        print(f"Inserted {len(result.inserted_ids)} documents with version {version}")
    
    return version
