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
import time
import pytz
from botocore.exceptions import ClientError

load_dotenv()

API_KEY = None
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

def build_service(api_key):
    global API_KEY
    API_KEY = api_key
    return(build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=api_key))

def get_all_api_keys():
    secret_name = "YoutubeAPIKeys"
    region_name = "us-east-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    
    return list(json.loads(get_secret_value_response['SecretString']).values())

#Logging initialization
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('YoutubeCommentersLogging')
logger.addHandler(watchtower.CloudWatchLogHandler())


#API credit counter for the Youtube Data API
current_units_used = 0
daily_quota_exceeded_keys = set()

#Build Youtube API connection
all_keys = get_all_api_keys()
logger.info(f"{len(all_keys)} keys in rotation")
youtube = None



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

def see_all_files_s3(bucket):
    s3 = boto3.resource('s3')
    s3_bucket = s3.Bucket(bucket)
    files = []
    for obj in s3_bucket.objects.all():
        files.append(obj.key)
    return files

#Sleeps until the next calendar day
def sleep_till_tomorrow():
    global current_units_used
    global daily_quota_exceeded_keys

    #Calculate seconds till tomorrow with datetime library in pacific timezone
    tz = pytz.timezone('America/Los_Angeles')
    tomorrow = datetime.datetime.replace(datetime.datetime.now(tz) + datetime.timedelta(days=1), 
                         hour=0, minute=0, second=0)
    delta = tomorrow - datetime.datetime.now(tz)

    #Log state
    logger.info(f"Quota exceeded, sleeping for {delta.seconds} seconds")
    logger.info(f"At time of sleep API credit counter is at {current_units_used}")

    #Sleep, reset credit counter, and return
    time.sleep(delta.seconds)
    current_units_used = 0
    daily_quota_exceeded_keys = set()
    return 

def rotate_keys():
    global youtube
    global daily_quota_exceeded_keys
    logger.info(f"Rotate Keys Called, currently on API Key {all_keys.index(API_KEY)}, length of quota_exceeded keys is {len(daily_quota_exceeded_keys)}")    
    daily_quota_exceeded_keys.add(API_KEY)
    for key in all_keys:
        if key not in daily_quota_exceeded_keys:
            logger.info("Swapping to new key")
            youtube = build_service(key)
            return
        else:
            logger.info(f'API Key {all_keys.index(key)} is in daily_exceeded keys')
    
    sleep_till_tomorrow()
    rotate_keys() 
    
def execute_youtube_list_query(youtube_service, **kwargs):
    global current_units_used
    current_units_used += 1

    try:
        response = youtube_service.list(
            **kwargs
        ).execute()

    except HttpError as e:
        #This likely occurs if there is no uploads or if the quota is exceeded
        logger.info(f"Error encountered, {e.resp.status}: {e.error_details[0]['reason']}")
        if(e.error_details[0]['reason'] == 'quotaExceeded'):
            rotate_keys()
            return None
        else:
            return None
    
    return response
    
#Gets the id of the playlist of uploads for a channel
def get_uploads_id_for_channel(youtube, channel_id) -> list:
    
    response = execute_youtube_list_query(youtube.channels(), part='contentDetails, statistics', id=channel_id)

    #If we recieve a valid response with items inside, return the id for the channel's playlist of uploads
    if('items' in response):
        if(response['items'][0]['statistics']['videoCount'] == '0'):
            logger.info(f'Channel id {channel_id} has no videos')
            return None
        else:
            uploads_id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
            return(uploads_id)
    else:
        logger.info(f'Could not find uploads id for channel id: {channel_id}')
        return None

#Gets recent videos, 50 at a time, for a given playlist id and pagination token 
def get_videos_for_uploads_id(youtube, uploads_id, next_page_token):
    #Ask youtube for recent videos from the playlist with a pagination token
    response = execute_youtube_list_query(youtube.playlistItems(),
        part="contentDetails",
        playlistId = uploads_id,
        pageToken = next_page_token,
        maxResults = 50
    )
        
    if (response is None):
        return [], None

    #Get a list of video ids in that playlist (youtube serves a max of 50 at a time)
    video_ids = [video_obj['contentDetails']['videoId'] for video_obj in response['items']]

    #If there is a pagination token in the repsonse, return that as well
    try:
        next_page_token = response['nextPageToken']
    except:
        next_page_token = None
        
    return(video_ids, next_page_token)

#Gets all commenters for a video up to max_commenters_per_channel
def get_commenters_for_video(youtube, video_id, channel_commenters, max_commenters_per_channel):
    commenters = set()
    next_page_token = ""

    #Loop through comment pages until nextPageToken field is blank, then return
    while(True):
        #Query youtube with relevant parameters for commentThreadLists
        response = execute_youtube_list_query(youtube.commentThreads(),
            part="snippet, replies",
            videoId = video_id,
            maxResults = 100,
            pageToken = next_page_token,
            moderationStatus = "published"   
        )

        if (response is None):
            return None

        #Iterate through commentThreads returned and grab author of topLevelComment and any replies
        for commentThread_obj in response['items']:
            if(len(channel_commenters | commenters) > max_commenters_per_channel):
                return(commenters)

            top_level_author = commentThread_obj['snippet']['topLevelComment']['snippet']['authorDisplayName']
            commenters.add(top_level_author)

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
                    commenters.add(comment_obj['snippet']['authorDisplayName'])
            except KeyError as e:
                pass

        #Attempt to get next_page_token, if blank return list of commenters
        if ('nextPageToken' in response):
            next_page_token = response['nextPageToken']
            #logger.info(f'Commneter list length: {len(commenters)}')
        else:
            return(commenters)

#Gets max_commenters_per_channel commenters for a given playlist of uploads (essentially for a given channel)
def get_commenters_for_uploads_id(uploads_id, max_commenters_per_channel):
    #Start each channel with an empty set of commenters and an empty pagination token (starts on first page)
    channel_commenters = set()
    video_next_page_token = ""

    #This loop continues to pull videos and commenters until we hit the limit max_commenters_per_channel
    while(True):

        #If we go through every video, the token will be None and there are no more commenters to find
        if(video_next_page_token is None):
            logger.info(f"There are not {max_commenters_per_channel} comments available, {len(channel_commenters)} commenters in set")
            return(channel_commenters)

        #Get a list of videos and a pagination token
        video_ids, video_next_page_token = get_videos_for_uploads_id(youtube, uploads_id, video_next_page_token)
        if(len(video_ids) == 0):
            logger.info(f'Empty videos list indicates playlistID was invalid, abandoning')
            return None

        logger.info(f'Successfully found videos')


        #For each video, get commenters. If the commenters for that video would put the total over the max, add one at time till max is reached
        comment_disabled_video_count = 0
        for video_id in video_ids:
            video_commenters = get_commenters_for_video(youtube, video_id, channel_commenters, max_commenters_per_channel)

            #If there are no video commenters that likely means comments are disabled. If we hit 10 of these, the channel will not be included.
            if(video_commenters == None):
                comment_disabled_video_count += 1
                if(comment_disabled_video_count > 10):
                    logger.info(f'Channel exceeded 10 comment disabled videos, abandoning')
                    return None
                else:
                    continue

            #Ensure exactly max_commenters_per_channel commenters in set
            if(len(channel_commenters | video_commenters) > max_commenters_per_channel):
                logger.info('Running logic to get exactly max commenters for channel')
                #Save commenters that arent in the master set and when the lengths of those two lists is equal to the max, update the master set and return
                unseen_commenters = set()
                for commenter in video_commenters:
                    if commenter not in channel_commenters:
                        unseen_commenters.add(commenter)
                        if(len(unseen_commenters) + len(channel_commenters) >= max_commenters_per_channel):
                            channel_commenters.update(unseen_commenters)
                            logger.info(f'Successfully finished comments for channel: commenter list length is {len(channel_commenters)}')
                            return(channel_commenters)
                    else:
                        continue

            #If this set won't put the total over the max, add them to the master set and go to the next video            
            else:
                channel_commenters.update(set(video_commenters))

def calculate_overlaps_for_channel(primary_channel, primary_channel_commenter_set, bucket):
    logger.info(f'Running comparisons for {primary_channel}')
    utility_files = ['ChannelIdMap.pkl', 'CurrentChannel.pkl', 'YoutubeUsernames.pkl']
    commenter_files = [file for file in see_all_files_s3(bucket) if file not in utility_files]
    channel_file_dict = {}
    for file_name in commenter_files:
        prefix_len = 9
        suffix_len = 17
        channel_name = file_name[prefix_len:-suffix_len]
        channel_file_dict[channel_name] = file_name

    len(channel_file_dict)

    overlap_dict = {}
    overlap_dict[primary_channel] = {}

    for comparison_channel, comparison_file in channel_file_dict.items():
        print(comparison_channel)

        comparison_channel_commenter_dict = load_pkl_obj_s3(comparison_file, bucket)

        shared_commenters = primary_channel_commenter_set & comparison_channel_commenter_dict[comparison_channel]
        shared_commenter_count = len(shared_commenters)
        overlap_dict[primary_channel][comparison_channel] = shared_commenter_count
    
    target_bucket = 'youtube-overlaps'
    dump_pkl_obj_s3(overlap_dict, f'June2021_{channel_username}_{len(overlap_dict[primary_channel])}_overlaps', target_bucket)

def find_processed_channels():
    utility_files = ['ChannelIdMap.pkl', 'CurrentChannel.pkl', 'YoutubeUsernames.pkl']
    commenter_files = [file for file in see_all_files_s3('youtube-commenters') if file not in utility_files]
    processed_channels = [file_name.split('_')[1] for file_name in commenter_files]
    return set(processed_channels)

#Iterates through channel list, resuming from where it left off if necessary, gets commenters, and dumps them into an s3 bucket
if __name__ == '__main__':
    logger.info('Youtube Commenters Scrape Started...')

    youtube_channels = load_pkl_obj_s3('YoutubeUsernames.pkl', 'youtube-commenters') #Load list of 20k youtube channels
    current_channel = load_pkl_obj_s3('CurrentChannel.pkl', 'youtube-commenters') #Get where we left off in case of crash
    channel_id_map = load_pkl_obj_s3('ChannelIdMap.pkl', 'youtube-commenters')

    processed_channels = find_processed_channels()

    starting_index = youtube_channels.index(current_channel) #Resume where we were by starting from loaded channel
    logger.info(f'Starting script at {starting_index}/{len(youtube_channels)}, current channel is {current_channel}')

    youtube = build_service(all_keys[0])

    max_commenters_per_channel = 20000

    saved_date = datetime.date.today()
    for i in range(starting_index, len(youtube_channels)): #iterate through list from starting point

        #Initalize channel values
        channel_username = youtube_channels[i]
        channel_id = channel_id_map[channel_username]

        #Ensures a channel is never proccessed twice, in case channels get skipped this lets me go back and start again without repeating work
        if(channel_username in processed_channels):
            logger.info(f'{channel_username} is already processed, moving to next channel')
            continue

        logger.info(f'Currently Using API KEY: {all_keys.index(API_KEY)}')

        #Update current channel file in case of crash of halt
        dump_pkl_obj_s3(channel_username, 'CurrentChannel.pkl', 'youtube-commenters')

        logging.info(f'Started Scrape for {channel_username}')
        
        uploads_id = get_uploads_id_for_channel(youtube, channel_id) #Each youtube channel has a playlist of uploads with a specific upload_id

        if(uploads_id is None): #This occurs when a youtube channel has no uploads ie. Youtube Movies
            continue

        logger.info(f'Successfully found uploads id for {channel_username}')

        channel_commenters = get_commenters_for_uploads_id(uploads_id, max_commenters_per_channel) #Gets videos and all commenters until max commenters per channel is hit

        if(channel_commenters is None): #This occurs for a number of reason, logged appropriately
            continue

        #Dump channel:commenters to S3 bucket for later analysis
        dump_pkl_obj_s3({channel_username: channel_commenters}, f'June2021_{channel_username}_{max_commenters_per_channel}_commenters', 'youtube-commenters')
        logger.info(f'Successfully dumped file for {channel_username}')
        logger.info(f'Completion Percentage: {(i/len(youtube_channels)) * 100}%')
        logger.info(f'Current API Credits Used: {current_units_used}')
        if(current_units_used > 10000):
            logger.info(f'Credits exceeded 10000 current values is {current_units_used}')

        calculate_overlaps_for_channel(channel_username, channel_commenters, 'youtube-commenters')


