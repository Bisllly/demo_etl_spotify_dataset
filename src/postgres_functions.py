from dotenv import load_dotenv
load_dotenv()
import psycopg2
import os
import io
import threading
from sql_queries import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

#Functions
def create_database():
  conn = psycopg2.connect(f"host=127.0.0.1 user={os.getenv('DWH_USER')} password={os.getenv('DWH_PWD')}")
  conn.set_session(autocommit=True)
  cur = conn.cursor()

  cur.execute("DROP DATABASE IF EXISTS music_dwh")
  cur.execute("CREATE DATABASE music_dwh WITH ENCODING 'utf8' TEMPLATE template0")

  conn.close()

  conn = psycopg2.connect(f"host=127.0.0.1 dbname=music_dwh user={os.getenv('DWH_USER')} password={os.getenv('DWH_PWD')}")
  cur = conn.cursor()

  return cur, conn


def drop_tables(cur, conn):
  for query in drop_table_queries:
    cur.execute(query)
    conn.commit()


def create_tables(cur, conn):
  for query in create_table_queries:
    cur.execute(query)
    conn.commit()



def insert_artists(df, cur, start_row, end_row):
    try:
        data_tuples = [tuple(x) for x in df[['artist_uri', 'artist_name']].iloc[start_row:end_row].values]
        cur.executemany(insert_into_artists, data_tuples)
    except Exception as e:
        print("An error occurred:", e)


def insert_albums(df, cur, start_row, end_row):
  try:
    data_tuples = [tuple(x) for x in df[['album_uri', 'album_name']].iloc[start_row:end_row].values]
    cur.executemany(insert_into_albums, data_tuples)
  except Exception as e:
    print("An error occurred:", e)


def insert_tracks(df, cur, start_row, end_row):
  try:
    data_tuples = [tuple(x) for x in df[['track_uri', 'track_name', 'artist_uri', 'album_uri', 'duration_ms']]
                   .iloc[start_row:end_row].values]
    cur.executemany(insert_into_tracks, data_tuples)
  except Exception as e:
    print("An error occurred", e)


def insert_playlists(df, cur, start_row, end_row):
  try:
    data_tuples = [tuple(x) for x in df[['playlist_pid', 'playlist_name', 'playlist_description']]
                   .iloc[start_row:end_row].values]
    cur.executemany(insert_into_playlists, data_tuples)
  except Exception as e:
    print("An error occurred", e)


def insert_playlist_tracks(df, cur, start_row, end_row):
  try:
    data_tuples = [tuple(x) for x in df[['playlist_pid', 'track_uri', 'pos']]
                   .iloc[start_row:end_row].values]
    cur.executemany(insert_into_playlist_tracks, data_tuples)
  except Exception as e:
    print("An error occurred", e)


def insert_execute(df, action, conn, cur):
  chunk_size = 70000
  threads = []
  try:
    for start_row in range(0, len(df), chunk_size):
      end_row = start_row + chunk_size
      thread = threading.Thread(target=action, args=(df, cur, start_row, end_row))
      thread.start()
      threads.append(thread)

    for thread in threads:
      thread.join()

    conn.commit()
  
  except Exception as e:
    print("An error occurred:", e)