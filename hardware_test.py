import os
import shutil

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

dataSet_path = "../Datasets/NYC_TaxiData"

# Try to read and print the whole Dataset (loads ~7ß GB into RAM) --> Fails - malloc!
#dataset = pq.ParquetDataset(dataSet_path + "/")
#table_partitioned = dataset.read()
#print(table_partitioned)

# Calculate Memory usage
import os
import pyarrow.parquet as pq


def calculate_memory_usage(dataSet_path, start_year, method="none"):
    total_bytes = 0

    for root, dirs, files in os.walk(dataSet_path):
        # Extract year from directory structure
        relative_path = os.path.relpath(root, dataSet_path)
        path_parts = relative_path.split(os.sep)

        # Skip root directory and check first partition level
        if len(path_parts) == 0 or path_parts[0] == '.':
            continue

        try:
            current_year = int(path_parts[0])  # First directory is year
            if current_year < start_year:
                continue  # Skip directories before start_year
        except (ValueError, IndexError):
            continue  # Skip non-year directories

        # Process files in valid directories
        for file in files:
            if file.endswith('.parquet'):
                file_path = os.path.join(root, file)

                if method == "metadata":
                    # Method 2: Metadata-based estimation
                    pf = pq.ParquetFile(file_path)
                    total_bytes += pf.metadata.total_uncompressed_size
                else:
                    # Method 1/3: Actual memory measurement
                    table = pq.read_table(file_path)
                    total_bytes += table.nbytes
                    del table

                print(f"Processed {file_path}")

    return total_bytes


# Configuration
dataSet_path = "../Datasets/NYC_TaxiData"
start_year = 2020  # Your target start year

# Choose method: "metadata" (fast) or "direct" (accurate)
total_bytes = calculate_memory_usage(dataSet_path, start_year, method="direct")

print(f"Estimated memory requirement: {total_bytes / (1024 ** 3):.2f} GB")