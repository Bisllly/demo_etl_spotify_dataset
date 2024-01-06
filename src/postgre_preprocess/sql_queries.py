# Drop tables
artists_table_drop = "DROP TABLE IF EXISTS artists"
albums_table_drop = "DROP TABLE IF EXISTS albums"
tracks_table_drop = "DROP TABLE IF EXISTS tracks"
playlists_table_drop = "DROP TABLE IF EXISTS playlists"
playlist_tracks_table_drop = "DROP TABLE IF EXISTS playlist_tracks"

# Create tables
artists_table_create = ('''
  CREATE TABLE IF NOT EXISTS artists (
      artist_uri VARCHAR PRIMARY KEY,
      artist_name VARCHAR
  )
''')

albums_table_create = ('''  
  CREATE TABLE IF NOT EXISTS albums (
      album_uri VARCHAR PRIMARY KEY,
      album_name VARCHAR
  )
''')

tracks_table_create = ('''
  CREATE TABLE IF NOT EXISTS tracks (
      track_uri VARCHAR PRIMARY KEY,
      track_name VARCHAR,
      artist_uri VARCHAR REFERENCES artists(artist_uri),
      album_uri VARCHAR REFERENCES albums(album_uri),
      duration_ms INT
  )
''')

playlists_table_create = ('''
  CREATE TABLE IF NOT EXISTS playlists (
      playlist_pid INT PRIMARY KEY,
      playlist_name VARCHAR,
      playlist_description TEXT
  )
''')

playlist_tracks_table_create = ('''
  CREATE TABLE IF NOT EXISTS playlist_tracks (
      playlist_pid INT REFERENCES playlists(playlist_pid),
      track_uri VARCHAR REFERENCES tracks(track_uri),
      pos INT
  )
''')

# insert into tables
insert_into_artists = ("""                 
  INSERT INTO artists (artist_uri, artist_name)
  VALUES (%s, %s)
  ON CONFLICT DO NOTHING
""")

insert_into_albums = ("""
  INSERT INTO albums (album_uri, album_name)
  VALUES (%s, %s)
  ON CONFLICT DO NOTHING
""")

insert_into_tracks = ("""
  INSERT INTO tracks (track_uri, track_name, artist_uri, album_uri, duration_ms)
  VALUES (%s, %s, %s, %s, %s)
  ON CONFLICT DO NOTHING
""")

insert_into_playlists = ("""
  INSERT INTO playlists (playlist_pid, playlist_name, playlist_description)
  VALUES (%s, %s, %s)
  ON CONFLICT DO NOTHING
""")

insert_into_playlist_tracks = ("""
  INSERT INTO playlist_tracks (playlist_pid, track_uri, pos)
  VALUES (%s, %s, %s)
  ON CONFLICT DO NOTHING
""")


# Query lists
create_table_queries = [artists_table_create, albums_table_create, 
                        tracks_table_create, playlists_table_create, playlist_tracks_table_create]
drop_table_queries = [artists_table_drop, albums_table_drop, 
                      tracks_table_drop, playlists_table_drop, playlist_tracks_table_drop]