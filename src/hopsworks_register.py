import hopsworks
import pandas as pd
from data_collection import collect_all_features
import os
from dotenv import load_dotenv

load_dotenv()

def register_data():
    # 1. Fetch the data
    city = "Karachi"
    df = collect_all_features(city)
    
    if df is not None:
        # 2. Connect to Hopsworks using your specific project name
        # Note: 'project' is case-sensitive!
        project = hopsworks.login(
            api_key_value=os.getenv("HOPSWORKS_API_KEY"),
            project="Pearls_Aqi_Predictor910" 
        )
        
        # 3. Get the Feature Store handle from the project
        fs = project.get_feature_store()

        # 4. Create or get the "Feature Group"
        aqi_fg = fs.get_or_create_feature_group(
            name="aqi_features",
            version=1,
            primary_key=['city'],
            event_time='date',
            description="AQI and Weather data for Karachi"
        )

        # 5. Upload (Insert) the data
        # 'write_options' helps avoid small metadata sync issues
        aqi_fg.insert(df, write_options={"wait_for_job": False})
        
        print("✅ Success! Data pushed to Hopsworks Feature Store.")

if __name__ == "__main__":
    register_data()