import pandas as pd
import os
from datetime import datetime
from tqdm import tqdm

# URL of dataset
url = "https://covid.ourworldindata.org/data/owid-covid-data.csv"

# Create folders
os.makedirs("../data/raw", exist_ok=True)
filename = f"../data/raw/owid_covid_data_{datetime.today().strftime('%Y-%m-%d')}.csv"

# Read and write in chunks to see progress
print("Downloading and writing in chunks...")

chunk_size = 100_000  # Adjust as needed
total_rows = 0

with pd.read_csv(url, chunksize=chunk_size) as reader:
    with open(filename, "w", encoding="utf-8") as f:
        for i, chunk in enumerate(tqdm(reader, desc="Downloading")):
            chunk.to_csv(f, index=False, header=(i == 0))  # Write header only once
            total_rows += len(chunk)


print(f" Finished! Total rows: {total_rows}")
print(f" Data saved to: {filename}")
