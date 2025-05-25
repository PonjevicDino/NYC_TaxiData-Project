import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
from pathlib import Path

try:
    pa.set_cpu_count(1)
except Exception as e:
    print(e)

dataset_root = "C:/Dataset_Modified"
output_file = './RQ2-PA.parquet'

all_pairs_tables = []
all_start_zones_tables = []

EMPTY_ZONE_PAIRS_SCHEMA = pa.schema([
    ('Start_Zone', pa.string()),
    ('End_Zone', pa.string()),
    ('Count', pa.int64()),
    ('Sum_Distance', pa.float64())
])

EMPTY_START_ZONES_SCHEMA = pa.schema([
    ('Start_Zone', pa.string()),
    ('Total_Count', pa.int64()),
    ('Total_Sum', pa.float64())
])

for parquet_file in Path(dataset_root).rglob("*.parquet"):
    print(f"Processing: {parquet_file.name}")

    table = pq.read_table(parquet_file)

    zone_combinations_temp = table.group_by(['Start_Zone', 'End_Zone']).aggregate([
        ('Trip_Distance', 'count'),
        ('Trip_Distance', 'sum')
    ])
    zone_combinations_temp = zone_combinations_temp.rename_columns([
        'Start_Zone', 'End_Zone', 'Count', 'Sum_Distance'
    ])
    all_pairs_tables.append(zone_combinations_temp)

    start_zone_group_temp = table.group_by('Start_Zone').aggregate([
        ('Trip_Distance', 'count'),
        ('Trip_Distance', 'sum')
    ])
    start_zone_group_temp = start_zone_group_temp.rename_columns([
        'Start_Zone', 'Total_Count', 'Total_Sum'
    ])
    all_start_zones_tables.append(start_zone_group_temp)


if all_pairs_tables:
    zone_pairs = pa.concat_tables(all_pairs_tables)
else:
    zone_pairs = pa.Table.from_pylist([], schema=EMPTY_ZONE_PAIRS_SCHEMA)

if zone_pairs.num_rows > 0:
    final_pairs_aggregated = zone_pairs.group_by(['Start_Zone', 'End_Zone']).aggregate([
        ('Count', 'sum'),
        ('Sum_Distance', 'sum')
    ])
    final_pairs = final_pairs_aggregated.rename_columns([
        'Start_Zone', 'End_Zone', 'Count', 'Sum_Distance'
    ])

    count_float = pc.cast(final_pairs['Count'], pa.float64())
    sum_dist_float = pc.cast(final_pairs['Sum_Distance'], pa.float64())
    avg_distance_col = pc.if_else(pc.equal(count_float, 0),
                                  pa.scalar(None, type=pa.float64()),
                                  pc.divide(sum_dist_float, count_float))
    final_pairs = final_pairs.append_column('Avg_Distance', avg_distance_col)
else:
    final_pairs_schema = EMPTY_ZONE_PAIRS_SCHEMA.append(pa.field('Avg_Distance', pa.float64()))
    final_pairs = pa.Table.from_pylist([], schema=final_pairs_schema)

if final_pairs.num_rows > 0:
    final_pairs_sorted = final_pairs.sort_by([
        ('Start_Zone', 'ascending'),
        ('Count', 'descending')
    ])

    table_with_row_index = final_pairs_sorted.add_column(
        final_pairs_sorted.num_columns,
        pa.field("original_row_index_temp", pa.int64()),
        [pa.array(range(final_pairs_sorted.num_rows), type=pa.int64())]
    )


    first_indices_per_group_table = table_with_row_index.group_by('Start_Zone').aggregate([
        ('original_row_index_temp', 'first')
    ])

    unique_row_indices_to_take = first_indices_per_group_table['original_row_index_temp_first']

    if not pa.types.is_integer(unique_row_indices_to_take.type):
        unique_row_indices_to_take = pc.cast(unique_row_indices_to_take, pa.int64())

    if len(unique_row_indices_to_take) > 0 and unique_row_indices_to_take.null_count < len(unique_row_indices_to_take):
        valid_indices_filter = pc.is_valid(unique_row_indices_to_take)
        valid_indices = unique_row_indices_to_take.filter(valid_indices_filter)

        if len(valid_indices) > 0:
            favorite_zones_temp = final_pairs_sorted.take(valid_indices)
            favorite_zones = favorite_zones_temp.select(['Start_Zone', 'End_Zone', 'Avg_Distance'])
            favorite_zones = favorite_zones.rename_columns(['Start_Zone', 'Favorite_End_Zone', 'Avg_Distance'])
        else:
            start_zone_type = final_pairs.schema.field('Start_Zone').type
            end_zone_type = final_pairs.schema.field('End_Zone').type
            favorite_zones_schema = pa.schema([
                ('Start_Zone', start_zone_type),
                ('Favorite_End_Zone', end_zone_type),
                ('Avg_Distance', pa.float64())
            ])
            favorite_zones = pa.Table.from_pylist([], schema=favorite_zones_schema)
    else:
        start_zone_type = final_pairs.schema.field(
            'Start_Zone').type if final_pairs.num_rows > 0 else EMPTY_ZONE_PAIRS_SCHEMA.field('Start_Zone').type
        end_zone_type = final_pairs.schema.field(
            'End_Zone').type if final_pairs.num_rows > 0 else EMPTY_ZONE_PAIRS_SCHEMA.field('End_Zone').type
        favorite_zones_schema = pa.schema([
            ('Start_Zone', start_zone_type),
            ('Favorite_End_Zone', end_zone_type),
            ('Avg_Distance', pa.float64())
        ])
        favorite_zones = pa.Table.from_pylist([], schema=favorite_zones_schema)

else:
    start_zone_type = EMPTY_ZONE_PAIRS_SCHEMA.field('Start_Zone').type
    end_zone_type = EMPTY_ZONE_PAIRS_SCHEMA.field('End_Zone').type
    favorite_zones_schema = pa.schema([
        ('Start_Zone', start_zone_type),
        ('Favorite_End_Zone', end_zone_type),
        ('Avg_Distance', pa.float64())
    ])
    favorite_zones = pa.Table.from_pylist([], schema=favorite_zones_schema)

if all_start_zones_tables:
    combined_start = pa.concat_tables(all_start_zones_tables)
else:
    combined_start = pa.Table.from_pylist([], schema=EMPTY_START_ZONES_SCHEMA)

if combined_start.num_rows > 0:
    start_zone_avg_aggregated = combined_start.group_by('Start_Zone').aggregate([
        ('Total_Count', 'sum'),
        ('Total_Sum', 'sum')
    ])
    start_zone_avg = start_zone_avg_aggregated.rename_columns([
        'Start_Zone', 'Total_Count', 'Total_Sum'
    ])

    total_count_float = pc.cast(start_zone_avg['Total_Count'], pa.float64())
    total_sum_float = pc.cast(start_zone_avg['Total_Sum'], pa.float64())
    avg_total_distance_col = pc.if_else(pc.equal(total_count_float, 0),
                                        pa.scalar(None, type=pa.float64()),
                                        pc.divide(total_sum_float, total_count_float))
    start_zone_avg = start_zone_avg.append_column('Avg_Total_Distance', avg_total_distance_col)
    start_zone_avg_selected = start_zone_avg.select(['Start_Zone', 'Avg_Total_Distance'])
else:
    start_zone_type = EMPTY_START_ZONES_SCHEMA.field('Start_Zone').type
    start_zone_avg_schema = pa.schema([
        ('Start_Zone', start_zone_type),
        ('Avg_Total_Distance', pa.float64())
    ])
    start_zone_avg_selected = pa.Table.from_pylist([], schema=start_zone_avg_schema)

if favorite_zones.num_rows == 0 and 'Start_Zone' not in favorite_zones.schema.names:
    schema_source_table = final_pairs if final_pairs.num_rows > 0 else zone_pairs
    start_zone_type = schema_source_table.schema.field(
        'Start_Zone').type if schema_source_table.num_rows > 0 and 'Start_Zone' in schema_source_table.schema.names else EMPTY_ZONE_PAIRS_SCHEMA.field(
        'Start_Zone').type
    end_zone_type = schema_source_table.schema.field(
        'End_Zone').type if schema_source_table.num_rows > 0 and 'End_Zone' in schema_source_table.schema.names else EMPTY_ZONE_PAIRS_SCHEMA.field(
        'End_Zone').type

    favorite_zones_schema = pa.schema([
        ('Start_Zone', start_zone_type),
        ('Favorite_End_Zone', end_zone_type),
        ('Avg_Distance', pa.float64())
    ])
    favorite_zones = pa.Table.from_pylist([], schema=favorite_zones_schema)

final_result = favorite_zones.join(
    start_zone_avg_selected,
    keys=['Start_Zone'],
    join_type='left outer'
)

try:
    pq.write_table(final_result, output_file)
except Exception as e:
    print(e)