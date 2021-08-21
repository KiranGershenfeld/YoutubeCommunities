import os
import json
from dotenv import load_dotenv
from apiclient.discovery import build
from apiclient.errors import HttpError

load_dotenv()
API_KEY = os.environ.get("YOUTUBE_APIKEY")

YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"


def build_service():
    return(build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=API_KEY))

def get_videos_for_channel(youtube_api, channel, numberOfVideos) -> list:
    response = youtube.channels().list(
        part="contentDetails",
        forUsername= channel,
        
    ).execute()

    uploads_id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    uploads_id = uploads_id[0:numberOfVideos]
    return(uploads_id)


if __name__ == "__main__":
    youtube = build_service()
    uploads = get_videos_for_channel(youtube, "PewDiePie", 10)
    print(uploads, len(uploads))
    