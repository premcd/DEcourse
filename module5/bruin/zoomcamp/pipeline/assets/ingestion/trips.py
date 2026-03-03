"""@bruin

name: ingestion.trips
connection: duckdb-default

materialization:
  type: table
  strategy: append
image: python:3.11

secrets:
  - key: duckdb-default
    inject_as: duckdb-default

columns:
  - name: pickup_datetime
    type: timestamp
    description: When the meter was engaged
  - name: dropoff_datetime
    type: timestamp
    description: When the meter was disengaged
  - name: taxi_type
    type: string
    description: Taxi service type (e.g., yellow, green)
  - name: extracted_at
    type: timestamp
    description: Timestamp when data was extracted (UTC)

@bruin"""

import io
import os
import json
from datetime import datetime

import pandas as pd
import requests
from dateutil import rrule

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def materialize():
    """
    Fetch NYC taxi trip data from the TLC public endpoint for the current run window.
    
    Uses BRUIN_START_DATE and BRUIN_END_DATE (YYYY-MM-DD format) to determine
    which months to fetch, and reads taxi_types from BRUIN_VARS pipeline variable.
    """
    start_date_str = os.environ["BRUIN_START_DATE"]
    end_date_str = os.environ["BRUIN_END_DATE"]
    
    # Parse dates (format: YYYY-MM-DD)
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    # Get taxi_types from pipeline variables (default to ["yellow"] if not set)
    bruin_vars = os.environ.get("BRUIN_VARS", "{}")
    vars_dict = json.loads(bruin_vars) if bruin_vars else {}
    taxi_types = vars_dict.get("taxi_types", ["yellow"])
    
    # Generate list of months between start and end dates
    months = list(rrule.rrule(rrule.MONTHLY, dtstart=start_date, until=end_date))
    
    # Fetch parquet files from TLC endpoint
    frames = []
    extracted_at = datetime.utcnow()
    
    for taxi_type in taxi_types:
        for month_start in months:
            # Format: {taxi_type}_tripdata_{year}-{month}.parquet
            # Example: yellow_tripdata_2022-03.parquet
            file_name = f"{taxi_type}_tripdata_{month_start.year}-{month_start.month:02d}.parquet"
            url = f"{BASE_URL}/{file_name}"
            
            try:
                # Fetch the parquet file
                response = requests.get(url, stream=True)
                if response.status_code != 200:
                    print(f"Skipping {url}: HTTP {response.status_code}")
                    continue
                
                # Read parquet into DataFrame
                buffer = io.BytesIO(response.content)
                df = pd.read_parquet(buffer)
                
                if df.empty:
                    continue
                
                # Add metadata columns
                df["taxi_type"] = taxi_type
                df["extracted_at"] = extracted_at
                
                frames.append(df)
                print(f"Successfully fetched {file_name}: {len(df)} rows")
                
            except Exception as e:
                print(f"Error fetching {url}: {e}")
                continue
    
    # Concatenate all DataFrames
    if not frames:
        # Return empty DataFrame with expected columns if no data found
        return pd.DataFrame()
    
    final_dataframe = pd.concat(frames, ignore_index=True)
    return final_dataframe
