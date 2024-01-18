import psycopg2
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import os
from dotenv import load_dotenv

nltk.download('punkt')
nltk.download('stopwords')

def get_song_recommendations(user_input):
    load_dotenv()
    features = process_input(user_input)

    conn = psycopg2.connect(f"host=127.0.0.1 dbname={os.getenv('DWH_DATABASE')} user={os.getenv('DWH_USER')} password={os.getenv('DWH_PWD')}")
    cur = conn.cursor()
    
    recommended_songs = find_matching_songs(features, cur)
    
    cur.close()
    conn.close()
    return recommended_songs


def process_input(input_text):
    # Tokenize the input text and remove stopwords
    stop_words = set(stopwords.words('english'))
    word_tokens = word_tokenize(input_text)
    keywords = [word.lower() for word in word_tokens if not word in stop_words and word.isalpha()]
    return keywords


def find_matching_songs(keywords, cur):

    if not keywords:
        return []

    query = "SELECT DISTINCT t.track_name FROM playlist_tracks pt "
    query += "JOIN playlists p ON pt.playlist_pid = p.playlist_pid "
    query += "JOIN tracks t ON pt.track_uri = t.track_uri WHERE "
    query += " OR ".join([f"LOWER(p.playlist_name) LIKE '%{keyword}%'" for keyword in keywords])
    print(query)

    cur.execute(query)
    rows = cur.fetchall()

    unique_song_names = list(set([row[0] for row in rows]))
    print(unique_song_names)
    return unique_song_names