import os
import json
import requests
import pandas as pd



if __name__ == '__main__':
    df = pd.read_csv("YoutubeTesting/Youtube.csv")
    df = pd.DataFrame([df["userID"], df["subscribers"], df["YouTube_Link-href"]]).transpose()
    print(df.head())

#Ok so I have a list of 4000 of the top subscribes youtube channels
#This is a legit chance to make a real youtube video

#This can be done in one shot so everything is gonna have to be pretty robust. 
#Its gonna be done through comment logs so Im going to have to download and store tons of data for this to work
#Its almost certainly infeasible to do that with my own computer its probably going to be done with GCP
#use data api to search most recent videos for channel
    #hit channel to get playlist id of uploads
    #hit videos with that playlist id
#use data api to get comments for 10 most recent videos
#go through the list and make a set with all the names of people that commented. pickle the set
#store that in gcp under folder for the channel
#do that for every channel
#crossover
#ez