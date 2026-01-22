import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

def get_mongo_client():
    uri = os.getenv("MONGO_URI")
    if not uri:
        raise ValueError("MONGO_URI not found in .env")
    client = MongoClient(uri)
    return client

if __name__ == "__main__":
    client = get_mongo_client()
    print(client.list_database_names())
