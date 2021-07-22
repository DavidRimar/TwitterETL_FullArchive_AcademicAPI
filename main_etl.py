import requests
import os
import json
from TweetCollector_FullArchiveAPI import TweetStreamer
from config import *

# tweetStreamer object
tweetStreamer = TweetStreamer(BEARER_TOKEN, DATABASE_URI_TRIAL)

# GET tweets with tweetStreamer
# It also handles data loading to DB via its tweetLoader
# set recreate_db = True if ran for the first time or need to recreate schema
# specify maximum number of tweets to return

start_time = "2021-05-10T20:13:37Z"
end_time = "2021-05-10T20:54:24Z"
query_placecountry = '-is:retweet lang:en place_country:GB'

"""
query_bbox = '-is:retweet lang:en bounding_box:[-2.812500 51.367494 -2.377167 51.591576]'
query_bbox_middleofnowhere = '-is:retweet lang:en bounding_box:[-3.462753 52.836995 -3.310661 52.914078]'
uk_bbox = '-7.57216793459 49.959999905 1.68153079591 58.6350001085'
bristol_bbox = '-2.812500 51.367494 -2.377167 51.591576'
middle_of_nowehere = '-3.462753 52.836995 -3.310661 52.914078' # Berwyn National Nature Reserve in Wales 
"""
query_hasgeo_uk = '-is:retweet has:geo lang:en -verdict -place_country:AG -place_country:BB -place_country:IE -place_country:AU -place_country:NZ -place_country:US -place_country:CA -place_country:JM -place_country:MT -place_country:TT -place_country:DM -place_country:VC)'


tweetStreamer.get_tweets(start_time, end_time, query_hasgeo_uk,
                         recreate_db=True, max_tweets=500)
