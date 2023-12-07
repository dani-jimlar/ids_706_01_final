# Databricks notebook source
#pip install --upgrade pip
!pip install spotipy
!pip install requests
!pip install numpy
!pip install os
!pip install time
!pip install datetime


# COMMAND ----------

import requests
import pandas as pd
import numpy as np
import urllib.parse
import spotipy.util as util
import PIL.Image
from pylab import *
#from dotenv import load_dotenv
import os
import time
from datetime import datetime

# COMMAND ----------

# scrap spotify top 100 us podcast to obtain list of podcasts and uri
headers = {
    "Referer": "https://podcastcharts.byspotify.com/us",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
}

params = {
    "region": "us",
}

response = requests.get(
    "https://podcastcharts.byspotify.com/api/charts/top", params=params, headers=headers
)

response_100 = response.json()

# transform json to pandas and add relevant columns
top_100 = pd.DataFrame(response_100)
top_100["showUri"] = top_100["showUri"].str.replace("spotify:show:", "")
top_100["rank"] = range(1, len(top_100) + 1)
top_100["chart_date"] = datetime.today().strftime("%Y-%m-%d")

# COMMAND ----------

client_id = 'edd85240c1604b1e89a3e01deb19b7e2'
client_secret = '8f7774169fab4bcabe1ce7e4e65623ef'
username = 'danjimnzlra'

scope = "user-library-read user-top-read"
redirect_uri = "https://developer.spotify.com/dashboard/applications/edd85240c1604b1e89a3e01deb19b7e2"
token = util.prompt_for_user_token(
    username=username,
    scope=scope,
    client_id=client_id,
    client_secret=client_secret,
    redirect_uri=redirect_uri,
)

# COMMAND ----------

token='BQDoH5Il02aAb0HZ4_HFehdcQ4iKUuSj21mQ5weLc9gphN9VUyJKzHzs-f-bIFJyf-4GEVxBP7Qn3wQj-uJFGi5C203T7F54A7ASzWpgOjn5XdZIvRG79S1NLYZP2g9n1C-MArheQJ1SnCdaeJaJ3olQ4qwOk8EbHMky9hURXR-Y67Pjvw3ZJwOmBzDXPiECVqQH9rmN3B-sDS6YMMQVOzgsmRfbEZZ24X8'

# COMMAND ----------

"""get number of episodes per top 200 shows and build data frame with info"""

# Setup
podcast_list = []
market = "US"

# Read
for i, pod in enumerate(top_100["showUri"]):
    # Create query
    query = "https://api.spotify.com/v1/shows/"
    query += f"{pod}?&market={market}"

    # Search
    response = requests.get(
        query,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    while (
        response.status_code != 200
    ):  # If query encounters error, sleep then try again
        # Sleep
        if "Retry-After" in response.headers:
            sleep_time = int(response.headers["Retry-After"]) + 10
        else:
            sleep_time = 30
        print(
            "Error at podcast",
            i + 1,
            "of",
            len(top_100),
            "! Will wait for",
            sleep_time,
            "seconds then try again...",
        )
        time.sleep(sleep_time)

        # Try again
        response = requests.get(
            query,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
        )

    # Save
    response_json = response.json()
    podcast_list.append(response_json)

print("Completed.\n")

top_100_n_eps = pd.DataFrame()

# Combine data from each podcast into the DataFrame
for pod in podcast_list:
    top_100_n_eps = pd.concat(
        [top_100_n_eps, pd.json_normalize(pod)], ignore_index=True
    )

#  Add ranking and date of rank
top_100_n_eps["ranking"] = range(1, len(top_100) + 1)
top_100_n_eps["chart_date"] = datetime.today().strftime("%Y-%m-%d")

# COMMAND ----------

"""Get jsons of all episodes from top 200 shows"""

# setup
episodes_list = []
podcastsID_list = []
market = "US"
limit = 50
offset = 0
episode_counter = 1  #

# Read
for i, row in top_100_n_eps[["id", "total_episodes"]].iterrows():
    pod = row["id"]
    episode_total = row["total_episodes"]

    while episode_counter <= episode_total:
        # Query
        query = f"https://api.spotify.com/v1/shows/{pod}/episodes?"
        query += f"market={market}&offset={offset}&limit={limit}"

        # Search
        response = requests.get(
            query,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
        )
        while (
            response.status_code != 200
        ):  # If query encounters error, sleep then try again
            # Sleep
            if "Retry-After" in response.headers:
                sleep_time = int(response.headers["Retry-After"]) + 10
            else:
                sleep_time = 30
            print(
                "Error at podcast",
                i + 1,
                "of",
                len(top_100_n_eps),
                "for episode",
                episode_counter,
                "of",
                episode_total,
                "! Will wait for",
                sleep_time,
                "seconds then try again...",
            )
            time.sleep(sleep_time)

            # Try again
            response = requests.get(
                query,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
            )

        # Save
        response_json = response.json()
        episodes_list.append(response_json)
        podcastsID_list.append(pod)

        # Update for next batch of episodes
        episode_counter += 50
        offset += 50

    # Reset for next podcast
    episode_counter = 1
    offset = 0

print("Completed.\n")

# Combine
episodes_info_dataset = pd.json_normalize(
    episodes_list[0]
)  # Create DataFrame structure using 1st episodes batch
episodes_info_dataset["show_id"] = podcastsID_list[0]
for i in range(1, len(episodes_list)):
    episodes_info_dataset = pd.concat(
        [episodes_info_dataset, pd.json_normalize(episodes_list[i])], ignore_index=True
    )
    episodes_info_dataset.loc[i, "show_id"] = podcastsID_list[i]

# COMMAND ----------

"""extract json info from each row and build dataframe of all episodes info"""
episodes_dataset = pd.json_normalize(
    episodes_info_dataset.loc[0, "items"]
)  # Use a temporary DataFrame as an intermediary
episodes_dataset["show_id"] = episodes_info_dataset.loc[
    0, "show_id"
]  # Create DataFrame structure using 1st batch of episodes

for i in range(1, len(episodes_info_dataset)):
    temp_df = pd.json_normalize(episodes_info_dataset.loc[i, "items"])
    temp_df["show_id"] = episodes_info_dataset.loc[i, "show_id"]
    episodes_dataset = pd.concat([episodes_dataset, temp_df], ignore_index=True)

# COMMAND ----------

'''build top 100 master dataset to merge with episodes dataset'''

top_100_s=top_100[['showUri', 'chartRankMove', 'showName', 'showPublisher', 'showImageUrl',
       'showDescription']
       ]
top_100_s=top_100_s.rename(columns={'chartRankMove':'showChartRankMove'})

top_100_n_eps_s=top_100_n_eps[['id','available_markets','languages','explicit','ranking', 'chart_date',
                               'episodes.total','external_urls.spotify']]


top_100_n_eps_s=top_100_n_eps_s.rename(columns={
'available_markets':'showAvailable_markets'
,'languages':'showLlanguages'
,'explicit':'show_explicit'
,'ranking':'show_ranking'
,'chart_date':'show_chart_date'
,'episodes.total':'show_episodes.total'
,'external_urls.spotify':'show_external_url'})

episodes_master= pd.merge(top_100_n_eps_s,top_100_s, how='left', left_on='id', right_on='showUri',)
episodes_master=episodes_master.drop(columns=['id',])
episodes_master=episodes_master.rename(columns={'showUri':'show_id'})


# COMMAND ----------

'''merge top 100 dataset with episodes dataset'''

episodes_dataset=episodes_dataset.rename(columns={																						
'audio_preview_url':'ep_audio_preview_url',
'description':'ep_description', 
'duration_ms':'ep_duration_ms',
'explicit':'ep_explicit',
'href':'ep_href',
'html_description':'ep_html_description',
'id':'ep_id',
'images':'ep_images', 
'is_externally_hosted':'ep_is_externally_hosted',
'is_playable':'ep_is_playable',
'language':'ep_language', 
'languages':'ep_languages', 
'name':'ep_name', 
'release_date':'ep_release_date',
'release_date_precision':'ep_release_date_precision',
'type':'ep_type', 
'uri':'ep_uri', 
'external_urls.spotify':'ep_external_urls',
'restrictions.reason':'ep_restrictions.reason'})

episodes_dataset_m=pd.merge(episodes_dataset, episodes_master, how='left',
                            on='show_id')

#episodes_dataset_m=episodes_dataset_m.drop(columns=["show_explicit"])	


columns_to_string = ['ep_audio_preview_url', 'ep_description', 'ep_explicit','ep_href','ep_duration_ms','ep_html_description', 'ep_id', 'ep_images', 'ep_is_externally_hosted','ep_is_playable', 'ep_language', 'ep_languages', 'ep_name',
'ep_release_date', 'ep_release_date_precision', 'ep_type', 'ep_uri','ep_external_urls','show_explicit']

# Transform specified columns to StringType
episodes_dataset_m[columns_to_string] = episodes_dataset_m[columns_to_string].astype(str)

# COMMAND ----------

episodes_dataset_m['ep_duration_ms'] = episodes_dataset_m['ep_duration_ms'].str.rstrip('.0').astype(float)



# COMMAND ----------

from pyspark.sql import SparkSession
# Create a Spark session
spark = SparkSession.builder.appName("podcast_app").getOrCreate()

spark_df = spark.createDataFrame(episodes_dataset_m)



# COMMAND ----------

# Define variables for table creation
table_name = "top_200_spotify_2"
checkpoint_path = "/tmp/your_checkpoint_path"
# Write the DataFrame to a Delta table
(spark_df.write
  .format("delta")
  .mode("overwrite")  # You can change the mode as needed
  .saveAsTable(table_name))

# COMMAND ----------

top_100_n_eps['total_episodes'].describe()


# COMMAND ----------

minutes=episodes_dataset_m['ep_duration_ms']/60000
minutes.describe()

