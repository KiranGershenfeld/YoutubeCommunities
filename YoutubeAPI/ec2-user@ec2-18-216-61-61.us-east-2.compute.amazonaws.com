import boto3
import pickle as pkl
import json
import os
from apiclient.discovery import build
from apiclient.errors import HttpError
from dotenv import load_dotenv
import logging
import watchtower
import datetime

#Youtube API Credentials
API_KEY = os.getenv("YOUTUBE_APIKEY")
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

#Logging initialization
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('YoutubeCommentersLogging')
logger.addHandler(watchtower.CloudWatchLogHandler())

current_units_used = 0

#S3 Helper Functions
def load_pkl_obj_s3(file_name, bucket):
    s3 = boto3.resource('s3')
    obj = pkl.loads(s3.Bucket(bucket).Object(file_name).get()['Body'].read())
    return obj

def dump_pkl_obj_s3(obj, file_name, bucket):
    s3 = boto3.resource('s3')
    pickle_obj = pkl.dumps(obj)
    response = s3.Object(bucket, file_name).put(Body=pickle_obj)
    return response['ResponseMetadata']

#Youtube Helper Functions
def build_service():
    return(build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=API_KEY))

def get_uploads_id_for_channel(youtube, channel) -> list:
    response = youtube.channels().list(
        part="contentDetails",
        forUsername= channel,
        
    ).execute()

    #Increment API Quota tracker
    global current_units_used
    current_units_used += 1

    if('items' in response):
        uploads_id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
        return(uploads_id)
    else:
        return None

def get_videos_for_uploads_id(youtube, uploads_id, next_page_token):
    try:
        response = youtube.playlistItems().list(
            part="contentDetails",
            playlistId = uploads_id,
            pageToken = next_page_token,
            maxResults = 50
        ).execute() 
        
        #Increment API Quota tracker
        global current_units_used
        current_units_used += 1

    except HttpError as e:
        print(f"Could not find playlist for channel, reason currently unknown. Error: {e}\n")
        return [], None



    #print(response)

    video_ids = [video_obj['contentDetails']['videoId'] for video_obj in response['items']]

    try:
        next_page_token = response['nextPageToken']
    except:
        next_page_token = None
        
    return(video_ids, next_page_token)

def get_commenters_for_video(youtube, video_id):
    commenters = []
    next_page_token = ""

    #Loop through comment pages until nextPageToken field is blank, then return
    while(True):
        #Query youtube with relevant parameters for commentThreadLists
        try:
            response = youtube.commentThreads().list(
                part="snippet, replies",
                videoId = video_id,
                maxResults = 100,
                pageToken = next_page_token,
                moderationStatus = "published"   
            ).execute() 

            #Increment API Quota tracker
            global current_units_used
            current_units_used += 1

        except HttpError as e:
            if(e.resp.status == 403):
                return None
            logger.info(f"Could not find comments for channel, reason currently unknown. Error: {e}\n")
            return None
        
        #Iterate through commentThreads returned and grab author of topLevelComment and any replies
        for commentThread_obj in response['items']:
            top_level_author = commentThread_obj['snippet']['topLevelComment']['snippet']['authorDisplayName']
            commenters.append(top_level_author)

            # CURRENTLY THIS SCRAPE MISSES SOME COMMENTS, TESTED HERE
            # if(top_level_author == 'DARK EYES GAMING'):
            #     try:
            #         print(len(commentThread_obj['replies']['comments']))
            #         for comment_obj in commentThread_obj['replies']['comments']:
                        
            #             print(comment_obj['snippet']['authorDisplayName'])
            #             commenters.append(comment_obj['snippet']['authorDisplayName'])
            #     except KeyError as e:
            #         print("This must be the other comment")
            #         pass
            
            #Replies field may not exits, if so just pass
            try:
                for comment_obj in commentThread_obj['replies']['comments']:
                    commenters.append(comment_obj['snippet']['authorDisplayName'])
            except KeyError as e:
                pass
        
        #Attempt to get next_page_token, if blank return list of commenters
        try:
            next_page_token = response['nextPageToken']
        except KeyError as e:
            return(commenters)

def get_commenters_for_uploads_id(uploads_id, max_commenters_per_channel):
    channel_commenters = set()
    video_next_page_token = ""

    while(True):
        if(video_next_page_token == None):
            logger.info(f"There are not {max_commenters_per_channel} comments available")
            break

        video_ids, video_next_page_token = get_videos_for_uploads_id(youtube, uploads_id, video_next_page_token)
        logger.info(f'Successfully found videos')

        for video_id in video_ids:
            video_commenters = get_commenters_for_video(youtube, video_id)
            if(video_commenters == None):
                return None

            if(len(channel_commenters | set(video_commenters)) > max_commenters_per_channel):
                for i in range(len(video_commenters)):
                    sub_video_commenters = set(video_commenters[:i])
                    if(len(channel_commenters | sub_video_commenters) >= max_commenters_per_channel):
                        channel_commenters.update(sub_video_commenters)
                        logger.info(f'Successfully finished comments for channel: commenter list length is {len(channel_commenters)}')
                        return(channel_commenters)
            else:
                channel_commenters.update(set(video_commenters))
                logger.info(f'Finished commenters for video: {video_id}, channel_commenters currently at: {len(channel_commenters)}/{max_commenters_per_channel}')

if __name__ == '__main__':
    logger.info('Youtube Commenters Scrape Started...')

    youtube_channels = load_pkl_obj_s3('YoutubeUsernames.pkl', 'youtube-commenters') #Load list of 20k youtube channels
    current_channel = load_pkl_obj_s3('CurrentChannel.pkl', 'youtube-commenters') #Get where we left off in case of crash
    channel_id_map = load_pkl_obj_s3('ChannelIdMap.pkl', 'youtube-commenters')

    starting_index = youtube_channels.index(current_channel) #Resume where we were by starting from loaded channel
    logger.info(f'Starting script at {starting_index}/{len(youtube_channels)}, current channel is {current_channel}')

    youtube = build_service()

    max_commenters_per_channel = 20000

    saved_date = datetime.date.today()
    for i in range(starting_index, len(youtube_channels)): #iterate through list from starting point
        if(datetime.date.today() != saved_date):
            current_units_used = 0
            saved_date = datetime.date.today()
            logging.info('New Date, reset credits used and updated saved date. This should only occur once every 24 hours')

        #Initalize channel values
        channel_username = youtube_channels[i]
        channel_id = channel_id_map[channel_username]

        #Update current channel file in case of crash of halt
        dump_pkl_obj_s3(channel_username, 'CurrentChannel.pkl', 'youtube-commenters')

        logging.info(f'Started Scrape for {channel_username}')
        
        uploads_id = get_uploads_id_for_channel(youtube, channel_username) #Each youtube channel has a playlist of uploads with a specific upload_id

        if(uploads_id is None): #This occurs when a youtube channel has no uploads ie. Youtube Movies
            logger.info(f'Could not find uploads id for {channel_username}')
            continue

        logger.info(f'Successfully found uploads id for {channel_username}')

        channel_commenters = get_commenters_for_uploads_id(uploads_id, max_commenters_per_channel) #Gets videos and all commenters until max commenters per channel is hit

        if(channel_commenters is None): #This occurs when a channel has comments turned off ie. CocoMelon
            logger.info(f'Comments are turned off for {channel_username}, or API quote has been hit see above log')
            continue

        #Dump channel:commenters to S3 bucket for later analysis
        dump_pkl_obj_s3({channel_username: channel_commenters}, f'June2021_{channel_username}_{max_commenters_per_channel}_commenters', 'youtube-commenters')
        logger.info(f'Successfully dumped file for {channel_username}')
        logger.info(f'Completion Percentage: {(i/len(youtube_channels)) * 100}%')
        logger.info(f'Current API Credits Used: {current_units_used}')
        if(current_units_used > 10000):
            logger.info(f'Credits exceeded 10000 current values is {current_units_used}')

