import pandas as pd

url = "../data/green_tripdata_2025-10.parquet"
df = pd.read_parquet(url)

# The direct count from the source
direct_count = len(df[df['trip_distance'] > 5.0])
print(f"Direct Pandas Count: {direct_count}")
