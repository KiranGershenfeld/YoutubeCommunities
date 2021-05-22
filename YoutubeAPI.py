import os
import json
from apiclient.discovery import build
from apiclient.errors import HttpError

CLIENT_SECRET_FILE = "YoutubeTesting/credentials.json"
API_KEY = json.load(open(CLIENT_SECRET_FILE))["APIKEY"]

YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

def build_service():
    return(build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=API_KEY))

get_upload_id_for_channel
if __name__ == "__main__":
    youtube = build_service()
    response = youtube.channels().list(
        part="contentDetails",
        forUsername="PewDiePie",
    ).execute()

    uploads_id = response["items"]["relatedPlaylists"]["uploads"]
    print(uploads_id)
