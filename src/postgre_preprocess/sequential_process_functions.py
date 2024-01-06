import os
import json
import pandas as pd
import time

def get_data_sequential() -> list:
    root_dir = os.getcwd() + "/data"
    json_files = []
    for dirpath, dirnames, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.endswith('.json'):
                file_path = os.path.join(dirpath, filename)
                json_files.append(file_path)
    return json_files

def process_file_sequential(file_path):
    def create_playlist_info(playlist):
        return {
            'playlist_pid': playlist.get('pid', -1),
            'playlist_name': playlist.get('name', ''),
            'playlist_description': playlist.get('description', '')
        }
    
    track_info = []
    with open(file_path, 'r') as f:
        json_data = json.load(f)

    for playlist in json_data['playlists']:
        playlist_info = create_playlist_info(playlist)
        for track in playlist.get('tracks', []):
            track_record = {**track, **playlist_info}
            track_info.append(track_record)

    return track_info

def flatten_structure_sequential(json_files: list) -> pd.DataFrame:
    all_tracks_flat = []
    for file in json_files:
        tracks = process_file_sequential(file)
        all_tracks_flat.extend(tracks)

    df_tracks = pd.DataFrame(all_tracks_flat)
    return df_tracks
