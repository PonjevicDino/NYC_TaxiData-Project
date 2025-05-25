import os
import pandas as pd

input_directory = "./"

for filename in os.listdir(input_directory):

    if filename.endswith(".parquet"):
        parquet_path = os.path.join(input_directory, filename)

        try:
            df = pd.read_parquet(parquet_path)

            base_name = os.path.splitext(filename)[0]
            csv_filename = f"{base_name}.csv"
            csv_path = os.path.join(input_directory, csv_filename)

            df.to_csv(csv_path, index=False)
            print(f"Successfully converted: {filename} -> {csv_filename}")

        except Exception as e:
            print(e)