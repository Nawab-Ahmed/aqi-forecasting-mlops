from pymongo import MongoClient
from datetime import datetime

def write_to_mongo(df, db_name="aqi_feature_store", collection_name="features_v1", version="v1", uri="mongodb://localhost:27017/"):
    client = MongoClient(uri)
    db = client[db_name]
    collection = db[collection_name]

    records = df.to_dict(orient="records")
    for r in records:
        r["version"] = version
        r["created_at"] = datetime.utcnow()
    
    # Clear old version
    collection.delete_many({"version": version})
    collection.insert_many(records)
    print(f"{len(records)} records inserted into MongoDB ({collection_name}).")
