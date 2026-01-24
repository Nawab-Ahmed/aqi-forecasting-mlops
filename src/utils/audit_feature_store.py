from utils.mongo_client import get_mongo_client
import pandas as pd

# 1️⃣ Connect to MongoDB
client = get_mongo_client()
db = client['aqi_feature_store']
collection = db['features_aqi_v1']

# 2️⃣ Count total documents
total_docs = collection.count_documents({})
print(f"Total documents in features_aqi_v1: {total_docs}")

# 3️⃣ Pull timestamps and features
docs = list(collection.find({}, {"event_timestamp": 1, "features.aqi": 1}))
df = pd.DataFrame(docs)
df['event_timestamp'] = pd.to_datetime(df['event_timestamp'])

# 4️⃣ Date range
start_date = df['event_timestamp'].min()
end_date = df['event_timestamp'].max()
print(f"Data range: {start_date} → {end_date}")

# 5️⃣ Daily coverage
df['date'] = df['event_timestamp'].dt.date
daily_counts = df.groupby('date').size()
print("Daily record counts:")
print(daily_counts)

# 6️⃣ Hourly coverage check
df['hour'] = df['event_timestamp'].dt.hour
hourly_counts = df.groupby(['date', 'hour']).size().unstack(fill_value=0)
print("Hourly coverage per day (0 means missing hour):")
print(hourly_counts)

# 7️⃣ Sample data
print("Sample feature row:")
print(df.head(3))
