import os
import pandas as pd
import numpy as np
from pathlib import Path

# Mapping from filename prefix to output Type
SERVICE_TYPE_MAP = {
    'yellow': 'Yellow',
    'green': 'Green',
    'fhv': 'FHV',
    'fhvhv': 'FHVHV'
}

ORIGINAL_ROOT = 'Dataset'
NEW_ROOT = 'Dataset_Modified'
COMPRESSION = 'gzip'  # Alternatives: 'gzip', 'snappy'


def process_parquet(file_path):
    filename = file_path.name
    service_key = filename.split('_')[0].lower()
    service_type = SERVICE_TYPE_MAP.get(service_key)

    if not service_type:
        print(f"Skipping unknown service type: {service_key} in {file_path}")
        return

    try:
        df = pd.read_parquet(file_path)
        new_df = pd.DataFrame()

        # Set Type column as string type with proper values
        new_df['Type'] = pd.Series([service_type] * len(df), dtype="string")

        # Common timestamp conversion
        def safe_convert(col):
            return pd.to_datetime(df[col], errors='coerce') if col in df.columns else pd.NaT

        if service_key == 'yellow':
            new_df['Vendor_ID'] = df['VendorID'].astype("string")
            new_df['Request_Time'] = pd.NaT
            new_df['OnScene_Time'] = pd.NaT
            new_df['Trip_Start'] = safe_convert('tpep_pickup_datetime')
            new_df['Trip_End'] = safe_convert('tpep_dropoff_datetime')
            new_df['Passengers'] = df['passenger_count'].fillna(-1).astype('Int32')
            new_df['Start_Zone'] = df['PULocationID'].fillna(0).astype('Int32')
            new_df['End_Zone'] = df['DOLocationID'].fillna(0).astype('Int32')
            new_df['Trip_Distance'] = df['trip_distance'].astype('float32')
            new_df['Payment_Type'] = df['payment_type'].fillna(-1).astype('Int32')
            new_df['Forward_Stored_Flag'] = df['store_and_fwd_flag'].map({'Y': True, 'N': False}).astype('boolean')
            new_df['Tip_Amount'] = df['tip_amount'].astype('float32')

        elif service_key == 'green':
            new_df['Vendor_ID'] = df['VendorID'].astype("string")
            new_df['Request_Time'] = pd.NaT
            new_df['OnScene_Time'] = pd.NaT
            new_df['Trip_Start'] = safe_convert('lpep_pickup_datetime')
            new_df['Trip_End'] = safe_convert('lpep_dropoff_datetime')
            new_df['Passengers'] = df['passenger_count'].fillna(-1).astype('Int32')
            new_df['Start_Zone'] = df['PULocationID'].fillna(0).astype('Int32')
            new_df['End_Zone'] = df['DOLocationID'].fillna(0).astype('Int32')
            new_df['Trip_Distance'] = df['trip_distance'].astype('float32')
            new_df['Payment_Type'] = df['payment_type'].fillna(-1).astype('Int32')
            new_df['Forward_Stored_Flag'] = df['store_and_fwd_flag'].map({'Y': True, 'N': False}).astype('boolean')
            new_df['Tip_Amount'] = df['tip_amount'].astype('float32')

        elif service_key == 'fhv':
            new_df['Vendor_ID'] = pd.NA
            new_df['Request_Time'] = pd.NaT
            new_df['OnScene_Time'] = pd.NaT
            new_df['Trip_Start'] = safe_convert('pickup_datetime')
            new_df['Trip_End'] = safe_convert('dropOff_datetime')
            new_df['Passengers'] = pd.NA
            new_df['Start_Zone'] = df.get('PUlocationID', pd.Series(0, index=df.index)).fillna(0).astype('Int32')
            new_df['End_Zone'] = df.get('DOLocationID', pd.Series(0, index=df.index)).fillna(0).astype('Int32')
            new_df['Trip_Distance'] = pd.NA
            new_df['Payment_Type'] = pd.NA
            new_df['Forward_Stored_Flag'] = pd.NA
            new_df['Tip_Amount'] = pd.NA

        elif service_key == 'fhvhv':
            new_df['Vendor_ID'] = df['hvfhs_license_num'].astype("string")
            new_df['Request_Time'] = safe_convert('request_datetime')
            new_df['OnScene_Time'] = safe_convert('on_scene_datetime')
            new_df['Trip_Start'] = safe_convert('pickup_datetime')
            new_df['Trip_End'] = safe_convert('dropoff_datetime')
            new_df['Passengers'] = df['shared_match_flag'].map({'Y': -2, 'N': -1}).astype('Int32')
            new_df['Start_Zone'] = df['PULocationID'].fillna(0).astype('Int32')
            new_df['End_Zone'] = df.get('DOLocationID', pd.Series(0, index=df.index)).fillna(0).astype('Int32')
            new_df['Trip_Distance'] = df['trip_miles'].astype('float32')
            new_df['Payment_Type'] = -1
            new_df['Forward_Stored_Flag'] = pd.NA
            new_df['Tip_Amount'] = df['tips'].astype('float32')

        # Calculate trip duration in seconds
        new_df['Trip_Duration'] = (
            (new_df['Trip_End'] - new_df['Trip_Start'])
            .dt.total_seconds()
            .astype('Int64')
        )

        # Create output path
        relative_path = file_path.relative_to(ORIGINAL_ROOT)
        output_path = Path(NEW_ROOT) / relative_path
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Save with compression
        new_df.to_parquet(
            output_path,
            index=False,
            compression=COMPRESSION,
            engine='pyarrow'
        )

        # Calculate sizes
        orig_size = file_path.stat().st_size
        new_size = output_path.stat().st_size
        ratio = (new_size / orig_size) * 100

        print(f"Processed: {file_path}")
        print(f"Original size: {orig_size / 1024:.2f} KB")
        print(f"New size: {new_size / 1024:.2f} KB")
        print(f"Size ratio: {ratio:.2f}%")
        print("---")

    except KeyError as e:
        print(f"Missing column {e} in file {file_path}")
    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")


# Main processing loop
for parquet_file in Path(ORIGINAL_ROOT).rglob('*.parquet'):
    if parquet_file.is_file():
        process_parquet(parquet_file)