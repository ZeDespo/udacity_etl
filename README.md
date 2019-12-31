# *SparkifyDB*

This repository's purpose is to perform ETL operations between the JSON log data that exists in some S3 bucket and 
the Redshift staging / analytical tables.

Sparkify's analysts wish to determine which songs each user is listening to, thus a star schema seemed the best option. 
The analysts would have all of the statistical information in a single fact table, and can retrieve 
any supplementary information, such as the song name, from any one of the four dimension tables. The log data is 
indiscriminately copied from the S3 bucket to the staging tables. From there, the data is cleaned and placed into 
logical analytic tables:

## Songplay

Holds all factual data for queries to perform JOINS onto. Comprised of data from both the events and songs JSON logs. 
_Contains some data not viewable in other tables (user_agent, session_id)_
```
songplay_id INT IDENTITY(0,1) PRIMARY KEY NOT NULL, 
start_time timestamp NOT NULL, 
user_id int NOT NULL, 
level varchar(4) NOT NULL,  # paid or free
song_id varchar,
artist_id varchar, 
session_id int NOT NULL, 
user_agent varchar NOT NULL, 
location varchar NOT NULL
```

## Songs

Each record pertains to a song by some artist / group. Made up solely from the song logs.
```
song_id varchar PRIMARY KEY NOT NULL, 
title varchar, 
artist_id varchar NOT NULL, 
year int, 
duration real
```

## Artists

Each record pertains to an artist or group that has recorded songs. Made up solely from the song logs.
```
artist_id varchar PRIMARY KEY NOT NULL,
name varchar, 
location varchar, 
latitude varchar, 
longitude varchar
```

## Time

Filtered by users actively listening to some song, holds information on all times users queried Sparkify. Made up solely from the 
event logs.
```
start_time timestamp PRIMARY KEY NOT NULL,
hour int,
day int, 
week int, 
month int, 
year int, 
weekday varchar
```


## Users
Filtered by users actively listening to some song, holds all user specific information. Made up solely from the 
event logs.
```
user_id int PRIMARY KEY NOT NULL,
first_name varchar,
last_name varchar,
gender varchar(1), 
level varchar(4) NOT NULL
```