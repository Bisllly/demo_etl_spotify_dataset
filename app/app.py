from flask import Flask, request, render_template
from src.queries_dwh.process import get_song_recommendations

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def index():
  if request.method == 'POST':
    user_input = request.form['user_input']
    recommended_songs = get_song_recommendations(user_input)
    return render_template('index.html', recommended_songs=recommended_songs)
  
  return render_template('index.html', recommended_songs=[])

if __name__ == '__main__':
  app.run(debug=True)