import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get('IAM_ROLE','ARN')
LOG_DATA = config.get('S3','LOG_DATA')
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')
SONG_DATA = config.get('S3','SONG_DATA')


# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events CASCADE"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs CASCADE"
songplay_table_drop = "DROP TABLE IF EXISTS songplays CASCADE"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender CHAR(1),
    itemInSession INT,
    lastName VARCHAR,
    length NUMERIC,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration NUMERIC,
    sessionId INT,
    song VARCHAR,
    status INT,
    ts TIMESTAMP,
    userAgent VARCHAR,
    userId INT);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INT, 
    artist_id VARCHAR, 
    artist_latitude NUMERIC, 
    artist_longitude NUMERIC, 
    artist_location VARCHAR, 
    artist_name VARCHAR, 
    song_id VARCHAR, 
    title VARCHAR,
    duration NUMERIC,
    year INT);
""")



songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT IDENTITY(0,1) SORTKEY PRIMARY KEY, 
    start_time TIMESTAMP REFERENCES time(start_time), 
    user_id INT REFERENCES users(user_id) DISTKEY, 
    level VARCHAR, 
    song_id VARCHAR REFERENCES songs(song_id), 
    artist_id VARCHAR REFERENCES artists(artist_id), 
    session_id INT, 
    location VARCHAR, 
    user_agent VARCHAR);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INT SORTKEY PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR, 
    gender CHAR(1), 
    level VARCHAR)
    DISTSTYLE ALL;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR SORTKEY PRIMARY KEY, 
    title VARCHAR, 
    artist_id VARCHAR, 
    year INT, 
    duration NUMERIC)
    DISTSTYLE ALL;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR SORTKEY PRIMARY KEY,
    name VARCHAR, 
    location VARCHAR, 
    latitude NUMERIC, 
    longitude NUMERIC)
    DISTSTYLE ALL;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP SORTKEY PRIMARY KEY, 
    hour INT, 
    day INT, 
    week INT, 
    month INT, 
    year INT, 
    weekday INT)
    DISTSTYLE ALL;
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM {}
IAM_ROLE {}
REGION 'us-west-2' FORMAT AS JSON {}
timeformat as 'epochmillisecs';
""").format(LOG_DATA,ARN,LOG_JSONPATH)

staging_songs_copy = ("""
COPY staging_songs
FROM {}
IAM_ROLE {}
REGION 'us-west-2' FORMAT AS JSON 'auto';
""").format(SONG_DATA,ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT  
        e.ts as start_time, 
        e.userId as user_id, 
        e.level, 
        s.song_id, 
        s.artist_id, 
        e.sessionId as session_id, 
        e.location,
        e.useragent as user_agent
    FROM staging_events e, staging_songs s
    WHERE e.song = s.title
    AND e.artist = s.artist_name
    AND page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
     SELECT DISTINCT 
        userId as user_id,
        firstName as first_name,
        lastName as last_name, 
        gender, 
        level
    FROM staging_events
    WHERE page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT 
        song_id,
        title,
        artist_id,
        year, 
        duration
    FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT 
        artist_id, 
        artist_name as name, 
        artist_location as location,
        artist_latitude as latitude, 
        artist_longitude as longitude
    FROM staging_songs;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
     SELECT DISTINCT 
        start_time, 
        extract(hour from start_time) as hour, 
        extract(day from start_time) as day, 
        extract(week from start_time) as week, 
        extract(month from start_time) as month, 
        extract(year from start_time) as year, 
        extract(dayofweek from start_time) as weekday
    FROM songplays;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
