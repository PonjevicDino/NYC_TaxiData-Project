from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from collections import defaultdict

dataset_root = "./Dataset_Modified"
output_file = "./RQ6.parquet"

sum_response = defaultdict(float)
count_response = defaultdict(int)

results = []

for parquet_file in Path(dataset_root).rglob("*.parquet"):
    print("Processing: " + parquet_file.name)
    if not parquet_file.name.__contains__("fhvhv"):
        continue;

    df = pq.read_table(parquet_file).to_pandas()

    df = df[df['Type'] == 'FHVHV'].copy()
    df['Request_Time'] = pd.to_datetime(df['Request_Time'])
    df['OnScene_Time'] = pd.to_datetime(df['OnScene_Time']).fillna(df['Trip_Start'])
    df['Response_Time'] = (df['OnScene_Time'] - df['Request_Time']).dt.total_seconds()

    df = df.dropna(subset=['Response_Time'])
    df = df[df['Response_Time'] >= 0]

    for vendor, time in zip(df['Vendor_ID'], df['Response_Time']):
        if not pd.isna(time):
            sum_response[vendor] += time
            count_response[vendor] += 1

for vendor in sum_response:
    if count_response[vendor] > 0:
        avg_time = sum_response[vendor] / count_response[vendor]
        results.append({
            'Vendor_ID': vendor,
            'Average_Response_Time': avg_time
        })

result_df = pd.DataFrame(results)

try:
    pq.write_table(pa.Table.from_pandas(result_df), output_file)
except Exception as e:
    print(e)