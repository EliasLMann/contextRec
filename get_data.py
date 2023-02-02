import spotipy
import pandas as pd
import numpy as np
from spotipy.oauth2 import SpotifyClientCredentials
import json
import os
from tqdm import tqdm
import re

#read cid from file
with open("ids/cid.txt", "r") as file:
    cid = file.read()

#read secret from file
with open("ids/secret.txt", "r") as file:
    secret = file.read()

#Authentication - without user
client_credentials_manager = SpotifyClientCredentials(client_id=cid, client_secret=secret)
sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)

#function to write the songs to a csv file
def write_songs_to_csv(directory = 'spotify_million_playlist_dataset/data/', write_directory = '',num_files = 1):
    file_names = os.listdir(directory)
    # opening the number of files specified in the range
    for i in range(0, num_files):
        with open(directory + file_names[i], "r") as file:

            #load the json file
            data = json.load(file)

            #iterate through the playlists
            for playlist in tqdm(data["playlists"], desc="Loading playlists"):

                playlist_data = []

                #retrieve the playlist name
                session = playlist["name"]

                #iterate through the tracks in the playlist
                for track in playlist["tracks"]:

                    #retrieve the track and artist data if possible
                    try:
                        track_obj = sp.track(track["track_uri"])
                    except:
                        continue

                    artist_obj = sp.artist(track["artist_uri"])

                    #creating the track audio features dictionary
                    track_data = sp.audio_features(track["track_uri"])[0]

                    #retrieving the artist and track names
                    artist_name = track["artist_name"]
                    track_name = track["track_name"]

                    #retrieving the artist and track popularity
                    artist_popularity = artist_obj['popularity']
                    track_popularity = track_obj["popularity"]

                    #retrieving the artist genres
                    artist_genres = artist_obj["genres"]

                    #create dictionary of track metadata
                    track_metadata = {
                        "artist_name": artist_name,
                        "track_name": track_name,
                        "artist_popularity": artist_popularity,
                        "track_popularity": track_popularity,
                        "artist_genres": artist_genres,
                        "playlist_name": session
                    }

                    #add track metadata to track features fo track data
                    track_data.update(track_metadata)
                    #add track data to playlist data list
                    playlist_data.append(track_data)

                #convert playlist list into a dataframe
                song_df = pd.DataFrame(playlist_data)
                #drop unnecessary columns
                song_df.drop(["analysis_url", "track_href", "type", 'uri'], axis=1, inplace=True)

                #get the playlist id
                pid = re.search('\d+', file_names[i])

                #write the dataframe to a csv file
                if int(playlist["pid"]) == int(pid.group()):
                    song_df.to_csv(write_directory + file_names[i] + '.csv', mode='w', header=True, index=False)
                else:
                    song_df.to_csv(write_directory + file_names[i] + '.csv', mode='a', header=False, index=False)

os.makedirs('playlist_dataframes', exist_ok=True) 

write_songs_to_csv(num_files=1, write_directory='playlist_dataframes/', directory='spotify_million_playlist_dataset/data/')