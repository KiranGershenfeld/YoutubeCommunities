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

bucket = 'youtube-commenters'

#Logging initialization
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('YoutubeCommentersLogging')
logger.addHandler(watchtower.CloudWatchLogHandler())

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
    objects = boto3.client('s3').list_objects_v2(Bucket=bucket)['Contents']
    files = []
    for obj in objects:
        files.append(obj['Key'])
    return files


utility_files = ['ChannelIdMap.pkl', 'CurrentChannel.pkl', 'YoutubeUsernames.pkl']
commenter_files = [file for file in see_all_files_s3(bucket) if file not in utility_files]
channel_file_dict = {file_name.split('_')[1]:file_name for file_name in commenter_files}
len(channel_file_dict)


processed_files = see_all_files_s3('youtube-overlaps')
processed_channels = set([file_name.split('_')[1] for file_name in processed_files])
logger.info(f'starting with {len(processed_channels)} processed channels')
exit()

count = 0

for primary_channel, _file in channel_file_dict.items():
    if primary_channel in processed_channels:
        continue

    primary_channel_commenter_dict = load_pkl_obj_s3(_file, bucket)
    
    overlap_dict = {}
    overlap_dict[primary_channel] = {}

    for comparison_channel, comparison_file in channel_file_dict.items():
        if comparison_channel == primary_channel:
            continue
        if comparison_channel in processed_channels:
            continue
        
        logger.info(f'comparisons for {primary_channel} and {comparison_channel}')

        comparison_channel_commenter_dict = load_pkl_obj_s3(comparison_file, bucket)

        shared_commenters = primary_channel_commenter_dict[primary_channel] & comparison_channel_commenter_dict[comparison_channel]
        shared_commenter_count = len(shared_commenters)
        overlap_dict[primary_channel][comparison_channel] = shared_commenter_count

    target_bucket = 'youtube-overlaps'
    dump_pkl_obj_s3(overlap_dict, f'June2021_{primary_channel}_{len(overlap_dict[primary_channel])}_overlaps', target_bucket)
    count += 1
    logger.info(f'dumped file for channel: {primary_channel}, finished {count}/{len(channel_file_dict)}')
    
    processed_channels.add(primary_channel)