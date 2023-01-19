import spotipy
import pandas as pd
import numpy as np
from spotipy.oauth2 import SpotifyClientCredentials, SpotifyOAuth

#read cid from file
with open("ids/cid.txt", "r") as file:
    cid = file.read()

#read secret from file
with open("ids/secret.txt", "r") as file:
    secret = file.read()

#Authentication - without user
client_credentials_manager = SpotifyClientCredentials(client_id=cid, client_secret=secret)
sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)


# create a pandas dataframe with the features of the songs in all the playlists
def get_playlist_features(playlist_URI):
    playlist_features_list = []
    playlist = sp.playlist(playlist_URI)
    for track in playlist["tracks"]["items"]:
        track_uri = track["track"]["uri"]
        playlist_features = sp.audio_features(track_uri)[0]
        playlist_features["playlist"] = playlist_URI

        playlist_features_list.append(playlist_features)

    playlist_features_df = pd.DataFrame(playlist_features_list)

    return playlist_features_df