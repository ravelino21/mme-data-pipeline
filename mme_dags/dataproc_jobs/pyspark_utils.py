import requests
import time, re
from urllib.parse import urlencode
import pyspark.sql.functions as F


class spotify_api():
    def __init__(self, credentials,token_url, version) -> None:
          self.credentials = credentials
        #   self.url = url
          self.token_url = token_url
          self.api_config = dict()
          self.version = version
    def _get_access_token(self):
        token_data = {
             "grant_type": "client_credentials"
        }
        token_headers ={
             "Authorization" : f"Basic {self.credentials.decode()}"
        }

        r = requests.post(self.token_url, data=token_data, headers=token_headers)
        access_token = r.json()['access_token']
        return access_token 
    
    def search(self, row):
        song_title = row['song_title']
        original_artist = row['original_artist']
        endpoint = f"https://api.spotify.com/{self.version}/search"
        access_token = self._get_access_token()
        headers = {'Authorization':f'Bearer {access_token}'}
     
        query = f'track:{song_title}'
        data = urlencode({
                'q':query,
                'type': 'track',
                'limit': '10'
                })
        url = f'{endpoint}?{data}'
        response = requests.get(url, headers=headers)
        flag = True
        retry_count = 0
        title_pattern = re.compile(rf"^{re.escape(song_title)}(\s*\(.*?version.*?\))?$", re.IGNORECASE)
        while flag:
            if response.status_code == 200:
    
                flag = False
                data = response.json().get('tracks', {}).get('items', [])
                data = [track for track in data if title_pattern.match(track['name'])]
                if data:
                    tracks_info = []
                    for track_info in data:
                        tracks_info.append({
                            'song_title': song_title,
                            'recordings_title': track_info['name'],
                            'album_name': track_info['album']['name'],
                            'artist_name': track_info['artists'][0]['name'],
                            'album_release_date': track_info['album']['release_date'],
                            'isrc': track_info['external_ids']['isrc'],
                            'is_playable': track_info.get('is_playable', False),
                            'popularity': track_info.get('popularity', 0)
                        })
                    return tracks_info
                else:
                    return [{
                            'song_title': song_title,
                            'recordings_title': None,
                            'album_name': None,
                            'artist_name': None,
                            'album_release_date': None,
                            'isrc': None,
                            'is_playable': None,
                            'popularity': None
                        }]
            else:
                time.sleep(300)
                retry_count += 1 
                print(f'retrying {retry_count} time')
                if retry_count > 4:
                    flag = False
                    raise Exception(f"API Error: {response.text}")



def youtube_api(api_key, version, row):
    endpoint = f'https://www.googleapis.com/youtube/{version}/search'
    song_title = row['song_title']
    artist_name = row['artist_name']
    data = urlencode({
        'key': api_key,
        'part': 'snippet',
        'q': f'{song_title} {artist_name}',
        'maxResults': 10
    })
    print(api_key)

    url = f'{endpoint}?{data}'
    print(url)
    response = requests.get(url)
    flag = True
    retry_count = 0
    while flag:
        if response.status_code == 200:
            flag = False
            data = response.json().get('items', [])
            if data:
                videos_info= []
                for video_info in data:
                    videos_info.append({
                        'song_title': song_title,
                        'artist_name': artist_name,
                        'isrc': row['isrc'],
                        'channel_id': video_info['snippet']['channelId'] if 'channelId' in video_info['snippet'] else None,
                        'title': video_info['snippet']['title'] if 'title' in video_info['snippet'] else None,
                        'channel_title': video_info['snippet']['channelTitle'] if 'channelTitle' in video_info['snippet'] else None,
                        'publish_time': video_info['snippet']['publishTime'] if 'publishTime' in video_info['snippet'] else None,
                        'video_id': video_info['id']['videoId'] if 'videoId' in video_info['id'] else None,
                        'description': video_info['snippet']['description'] if 'description' in video_info['snippet'] else None
                    })
                return videos_info
            else:
                return [{
                        'song_title': song_title,
                        'artist_name': artist_name,
                        'isrc': row['isrc'],
                        'channel_id' : None,
                        'title': None,
                        'channel_title': None,
                        'publish_time': None,
                        'video_id': None,
                        'description': None
                    }]
        else:
            time.sleep(300)
            retry_count += 1 
            print(f'retrying {retry_count} time')
            if retry_count > 4:
                flag = False
                raise Exception(f"API Error: {response.text}")