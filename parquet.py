import os
import shutil

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

# create array from list of values
animal = pa.array(["sheep", "cows", "horses", "foxes", "sheep", "cows", "horses", "foxes"], type=pa.string())
count = pa.array([12, 5, 2, 1, 12, 5, 3, 5], type=pa.int8())
year = pa.array([2022, 2022, 2022, 2022, 2023, 2023, 2023, 2023], type=pa.int16())

# turn this into a table
table = pa.Table.from_arrays([animal, count, year], names=["animal", "count", "year"])
print(table)

print(pa.TableGroupBy(table, "animal").aggregate([("count", "sum")]))
print(pa.TableGroupBy(table, "year").aggregate([("count", "sum")]))

# save as parquet file
pq.write_table(table, "example.parquet")

# load parquet file
table2 = pq.read_table("example.parquet")
print(table2)

count_y = pc.value_counts(table2['animal'])
print(count_y)

parquet_file = pq.ParquetFile("example.parquet")
# check the orc file contents
print("Metadata:")
print(parquet_file.metadata)
print("Schema:")
print(parquet_file.schema)
print("Metadata for Row Group:")
print(parquet_file.metadata.row_group(0))
print("Metadata for Row Group Column:")
print(parquet_file.metadata.row_group(0).column(0))

# read specific file
print(parquet_file.read_row_group(0))

# attempt to get more row groups (1 table == at least 1 row group)
with pq.ParquetWriter('example2.parquet', table.schema) as writer:
    for i in range(10):
        writer.write_table(table)
pf2 = pq.ParquetFile("example.parquet")
print("Metadata:")
print(pf2.metadata)
print("Schema:")
print(pf2.schema)
print("Metadata for Row Group:")
print(pf2.metadata.row_group(0))

# write partition
root_path = "example_parquet_partitions"
shutil.rmtree(root_path, ignore_errors=True)
pq.write_to_dataset(table, root_path=root_path, partition_cols=['year', 'animal'])

# read Partitioned Parquet File
dataset = pq.ParquetDataset(root_path + "/")
table_partitioned = dataset.read()
print(table_partitioned)

# WRITE METADATA
metadata_collector = []
pq.write_to_dataset(table, root_path=root_path, metadata_collector=metadata_collector)
pq.write_metadata(table.schema, root_path + "/_common_metadata")
pq.write_metadata(table.schema, root_path + "/_metadata", metadata_collector = metadata_collector)

# read a real Parquet file
tng = pq.ParquetFile('scripts_tng.parquet')
print(tng.metadata)
print(tng.schema)
#print(tng.read())
table_tng = pq.read_table('scripts_tng.parquet')

# count words
result = pc.value_counts(pc.list_flatten(pc.utf8_split_whitespace(table_tng["scene_details"])))
result = result.sort(by="counts", order="descending")
print(result)
print(pa.TableGroupBy(table_tng, keys="episode_id").aggregate(
    [(["scene_id"], "count")]
))