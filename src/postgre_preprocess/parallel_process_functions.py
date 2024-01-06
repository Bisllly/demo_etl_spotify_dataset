import os
import json
import pandas as pd
import pyarrow.parquet as pq
from multiprocessing import Pool
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor


def get_data() -> list:
  root_dir = os.getcwd() + "/data"
  return [os.path.join(root_dir, f) for f in os.listdir(root_dir) if f.endswith('.json')]


def read_and_process_data(file_path):

  # process each file individually to reduce memory load
  processed_data = []
  with open(file_path, 'r') as f:
    json_data = json.load(f)

    for playlist in json_data['playlists']:
      playlist_info = {
        'playlist_pid' : playlist.get('pid', -1),
        'playlist_name' : playlist.get('name', ''),
        'playlist_description' : playlist.get('description', '')
      }
      for track in playlist.get('tracks', []):
        track_record = {**track, **playlist_info}
        processed_data.append(track_record)

  # write processed data to a temporary CSV file to avoid large memory usage
  temp_filename = f"temp_{os.path.basename(file_path)}.csv"
  pd.DataFrame(processed_data).to_csv(temp_filename, index=False)
  return temp_filename
  
def flatten_structure(json_files: list) -> pd.DataFrame:
  num_processes = 8
  
  # use Pool for CPU-bound tasks (processing data)
  with Pool(num_processes) as pool:
    temp_files = pool.map(read_and_process_data, json_files)

  df_tracks = pd.concat([pd.read_csv(f) for f in temp_files], ignore_index=True)

  for f in temp_files:
    os.remove(f)

  return df_tracks

def clean_data(df_tracks: pd.DataFrame) -> pd.DataFrame:
    columns_to_clean = ['artist_name', 'track_name', 'album_name', 'playlist_name', 'playlist_description']
    
    regex_special_chars = '[^a-zA-Z0-9\s,.\'-]'

    for col in columns_to_clean:
        if col in df_tracks.columns:
            df_tracks[col] = df_tracks[col].str.replace(regex_special_chars, '', regex=True)

    return df_tracks

def process_chunk(parquet_file_path, batch_size, start):
    parquet_file = pq.ParquetFile(parquet_file_path)
    table = parquet_file.read_row_group(start)
    return table.to_pandas()

def read_parquet_to_df(parquet_file_path, batch_size=12000, max_workers=4):
    parquet_file = pq.ParquetFile(parquet_file_path)
    num_row_groups = parquet_file.num_row_groups

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_chunk, parquet_file_path, batch_size, i) 
                   for i in range(num_row_groups)]
        dfs = [future.result() for future in futures]

    return pd.concat(dfs, ignore_index=True)



def read_parquet_to_spark_df(spark, parquet_file_path):
   spark_df = spark.read.parquet(parquet_file_path)
   return spark_df


def write_to_db(filtered_spark_df, table_name, jdbc_url, properties):
  num_partitions = 5
  try:
    repartitioned_df = filtered_spark_df.repartition(num_partitions)
    repartitioned_df.write \
      .format("jdbc") \
      .option("url", jdbc_url) \
      .option("dbtable", table_name) \
      .option("user", properties['user']) \
      .option("password", properties['password']) \
      .option("driver", "org.postgresql.Driver") \
      .mode("append") \
      .save()
  except Exception as e:
     print("An error occured:", e)