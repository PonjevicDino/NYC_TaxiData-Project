from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

dataset_root = "C:/Dataset_Modified"
output_file = "./RQ5-PA.parquet"

all_dist_aggregations = []
all_dur_aggregations = []

SCHEMA_DIST_AGG_FILE = pa.schema([
    ('Type', pa.string()),
    ('Sum_TPD_File', pa.float64()),
    ('Count_TPD_File', pa.int64())
])

SCHEMA_DUR_AGG_FILE = pa.schema([
    ('Type', pa.string()),
    ('Sum_TPDur_File', pa.float64()),
    ('Count_TPDur_File', pa.int64())
])

for parquet_file in Path(dataset_root).rglob("*.parquet"):
    print(f"Processing: {parquet_file.name}")

    table = pq.read_table(parquet_file)

    trip_distance_float = pc.cast(table['Trip_Distance'], pa.float64())
    trip_distance_processed = pc.if_else(pc.equal(trip_distance_float, 0.0),
                                         pa.scalar(None, type=pa.float64()),
                                         trip_distance_float)

    trip_duration_float = pc.cast(table['Trip_Duration'], pa.float64())
    trip_duration_processed = pc.if_else(pc.equal(trip_duration_float, 0.0),
                                         pa.scalar(None, type=pa.float64()),
                                         trip_duration_float)

    tip_amount_float = pc.cast(table['Tip_Amount'], pa.float64())

    tip_per_distance_calc = pc.divide(tip_amount_float, trip_distance_processed)
    tip_per_duration_calc = pc.divide(tip_amount_float, trip_duration_processed)

    temp_table_for_agg = pa.Table.from_arrays([
        table['Type'],
        tip_per_distance_calc,
        tip_per_duration_calc
    ], names=['Type', 'Tip_Per_Distance_Calc', 'Tip_Per_Duration_Calc'])

    agg_dist_file = temp_table_for_agg.group_by('Type').aggregate([
        ('Tip_Per_Distance_Calc', 'sum'),
        ('Tip_Per_Distance_Calc', 'count')
    ])
    agg_dist_file = agg_dist_file.rename_columns([
        'Type', 'Sum_TPD_File', 'Count_TPD_File'
    ])
    all_dist_aggregations.append(agg_dist_file)

    agg_dur_file = temp_table_for_agg.group_by('Type').aggregate([
        ('Tip_Per_Duration_Calc', 'sum'),
        ('Tip_Per_Duration_Calc', 'count')
    ])
    agg_dur_file = agg_dur_file.rename_columns([
        'Type', 'Sum_TPDur_File', 'Count_TPDur_File'
    ])
    all_dur_aggregations.append(agg_dur_file)

if all_dist_aggregations:
    combined_dist_agg = pa.concat_tables(all_dist_aggregations)
else:
    combined_dist_agg = pa.Table.from_pylist([], schema=SCHEMA_DIST_AGG_FILE)

final_dist_agg_temp = combined_dist_agg.group_by('Type').aggregate([
    ('Sum_TPD_File', 'sum'),
    ('Count_TPD_File', 'sum')
])
final_dist_agg = final_dist_agg_temp.rename_columns([
    'Type', 'Total_Sum_Tip_Per_Distance', 'Total_Count_Tip_Per_Distance'
])

if all_dur_aggregations:
    combined_dur_agg = pa.concat_tables(all_dur_aggregations)
else:
    combined_dur_agg = pa.Table.from_pylist([], schema=SCHEMA_DUR_AGG_FILE)

final_dur_agg_temp = combined_dur_agg.group_by('Type').aggregate([
    ('Sum_TPDur_File', 'sum'),
    ('Count_TPDur_File', 'sum')
])
final_dur_agg = final_dur_agg_temp.rename_columns([
    'Type', 'Total_Sum_Tip_Per_Duration', 'Total_Count_Tip_Per_Duration'
])


merged_agg = final_dist_agg.join(final_dur_agg, keys=['Type'], join_type='full outer')

sum_tpd_col = merged_agg['Total_Sum_Tip_Per_Distance']
count_tpd_col = merged_agg['Total_Count_Tip_Per_Distance']
is_count_dist_invalid = pc.or_(
    pc.is_null(count_tpd_col),
    pc.equal(count_tpd_col, pa.scalar(0, type=count_tpd_col.type if count_tpd_col.type != pa.null() else pa.int64()))
)
factor_mile_col = pc.if_else(
    is_count_dist_invalid,
    pa.scalar(None, type=pa.float64()),
    pc.divide(pc.cast(sum_tpd_col, pa.float64()), pc.cast(count_tpd_col, pa.float64()))
)

sum_tpdur_col = merged_agg['Total_Sum_Tip_Per_Duration']
count_tpdur_col = merged_agg['Total_Count_Tip_Per_Duration']
is_count_dur_invalid = pc.or_(
    pc.is_null(count_tpdur_col),
    pc.equal(count_tpdur_col, pa.scalar(0, type=count_tpdur_col.type if count_tpdur_col.type != pa.null() else pa.int64()))
)
avg_tip_per_dur = pc.if_else(
    is_count_dur_invalid,
    pa.scalar(None, type=pa.float64()),
    pc.divide(pc.cast(sum_tpdur_col, pa.float64()), pc.cast(count_tpdur_col, pa.float64()))
)
factor_min_col = pc.multiply(avg_tip_per_dur, pa.scalar(60.0, type=pa.float64()))

current_schema_names = merged_agg.schema.names
if 'Factor_Tip_Mile' not in current_schema_names:
     merged_agg = merged_agg.append_column(pa.field('Factor_Tip_Mile', factor_mile_col.type), factor_mile_col)
else:
     merged_agg = merged_agg.drop_columns(['Factor_Tip_Mile']).append_column(pa.field('Factor_Tip_Mile', factor_mile_col.type), factor_mile_col)

if 'Factor_Tip_Minute' not in current_schema_names:
    merged_agg = merged_agg.append_column(pa.field('Factor_Tip_Minute', factor_min_col.type), factor_min_col)
else:
    merged_agg = merged_agg.drop_columns(['Factor_Tip_Minute']).append_column(pa.field('Factor_Tip_Minute', factor_min_col.type), factor_min_col)

result_table = merged_agg.select(['Type', 'Factor_Tip_Mile', 'Factor_Tip_Minute'])

try:
    pq.write_table(result_table, output_file)
except Exception as e:
    print(e)