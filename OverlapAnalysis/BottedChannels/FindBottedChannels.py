from re import T
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

def main():
    utility_files = ['CurrentChannel.pkl', 'ChannelIdMap.pkl', 'YoutubeUsernames.pkl']
    all_files = [file for file in see_all_files_s3('youtube-commenters') if file not in utility_files]
    channel_file_dict = {}
    for file_name in all_files:
        prefix_len = 9
        suffix_len = 17
        channel_name = file_name[prefix_len:-suffix_len]
        channel_file_dict[channel_name] = file_name

    channel_total_overlap_map = {}
    counter = 0
    for channel, file in channel_file_dict.items():
        try:
            num_channel_commenters = len(load_pkl_obj_s3(file, 'youtube-commenters')[channel])
            channel_total_overlap_map[channel] = num_channel_commenters
            counter += 1
            print(f'Channel: {channel} has {num_channel_commenters} commenters, progress {counter}/{len(channel_file_dict)}')
        except Exception as e:
            print(f'Could not find length of channel commeners, error: {e}')
    
    with open('ChannelTotalOverlapMap.pkl', 'wb') as handle:
        pkl.dump(channel_total_overlap_map, handle)

if __name__ == '__main__':
    main()
    

