import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSON_PATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
ARN = config.get("IAM_ROLE", "ARN")


# DROP TABLES

staging_events_table_drop = "DROP table IF EXISTS staging_events;"
staging_songs_table_drop = "DROP table IF EXISTS staging_songs;"
songplay_table_drop = "DROP table IF EXISTS songplays;"
user_table_drop = "DROP table IF EXISTS users;"
song_table_drop = "DROP table IF EXISTS songs;"
artist_table_drop = "DROP table IF EXISTS artists"
time_table_drop = "DROP table IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events
(
    artist          varchar,
    auth            varchar, 
    firstName       varchar,
    gender          varchar,   
    itemInSession   int,
    lastName        varchar,
    length          FLOAT,
    level           varchar, 
    location        varchar,
    method          varchar,
    page            varchar,
    registration    varchar,
    sessionId       int,
    song            varchar,
    status          int,
    ts              timestamp,
    userAgent       varchar,
    userId          int
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs
(
  num_songs int,
  artist_id varchar,
  artist_latitude FLOAT,
  artist_longitude FLOAT,
  artist_location varchar,
  artist_name varchar,
  song_id varchar,
  title varchar,
  duration FLOAT,
  year int
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id       int         distkey,
    first_name    varchar    ,
    last_name     varchar    ,
    gender        varchar  ,
    level         varchar  ,
    PRIMARY KEY (user_id)
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id   varchar                    sortkey,
    title     varchar          NOT NULL,
    artist_id varchar          NOT NULL,
    year        int           ,
    duration    FLOAT       ,
    PRIMARY KEY (song_id)
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artist_id   varchar                  sortkey,
    name        varchar        NOT NULL,
    location    varchar        ,
    latitude    FLOAT        ,
    longitude    FLOAT        ,
    PRIMARY KEY (artist_id)
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time    timestamp sortkey,
    hour            int     NOT NULL,
    day             int     NOT NULL,
    week            int     NOT NULL,
    month           int     NOT NULL,
    year            int     NOT NULL,
    weekday         int     NOT NULL,
    PRIMARY KEY (start_time)
);
""")

songplay_table_create = ("""
CREATE TABLE songplays(
    songplay_id   int             IDENTITY (0,1),
    start_time    timestamp       REFERENCES  time(start_time)    sortkey,
    user_id       int             REFERENCES  users(user_id) distkey,
    level           varchar       ,
    song_id       varchar         REFERENCES  songs(song_id),
    artist_id     varchar         REFERENCES  artists(artist_id),
    session_id    int             NOT NULL,
    location        varchar       ,
    user_agent    varchar         ,
    PRIMARY KEY (songplay_id)
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events 
    FROM {} 
    iam_role {} 
    region 'us-west-2'
    FORMAT AS JSON {} 
    timeformat 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(LOG_DATA, ARN, LOG_JSON_PATH)

staging_songs_copy = ("""
COPY staging_songs 
    FROM {}
    iam_role {}
    region 'us-west-2'
    FORMAT AS JSON 'auto' 
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(SONG_DATA, ARN)

# FINAL TABLES

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId, 
                firstName, 
                lastName, 
                gender, 
                level
FROM staging_events
WHERE userId IS NOT NULL AND page  =  'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT  DISTINCT(song_id) AS song_id,
        title,
        artist_id,
        year,
        duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT  DISTINCT(artist_id) AS artist_id,
        artist_name         AS name,
        artist_location     AS location,
        artist_latitude     AS latitude,
        artist_longitude    AS longitude
FROM staging_songs
WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT  DISTINCT(start_time)                AS start_time,
        EXTRACT(hour FROM start_time)       AS hour,
        EXTRACT(day FROM start_time)        AS day,
        EXTRACT(week FROM start_time)       AS week,
        EXTRACT(month FROM start_time)      AS month,
        EXTRACT(year FROM start_time)       AS year,
        EXTRACT(dayofweek FROM start_time)  as weekday
FROM songplays;
""")

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
SELECT DISTINCT (e.ts) as start_time, 
                e.userId, 
                e.level, 
                s.song_id, 
                s.artist_id, 
                e.sessionId, 
                e.location, 
                e.userAgent
FROM staging_events e 
JOIN staging_songs s 
    ON (e.song = s.title AND e.artist = s.artist_name AND e.length = s.duration)
WHERE e.page = 'NextSong';
""")

# GET NUMBER OF ROWS IN EACH TABLE
get_number_staging_events = ("""
    SELECT COUNT(*) FROM staging_events
""")

get_number_staging_songs = ("""
    SELECT COUNT(*) FROM staging_songs
""")

get_number_songplays = ("""
    SELECT COUNT(*) FROM songplays
""")

get_number_users = ("""
    SELECT COUNT(*) FROM users
""")

get_number_songs = ("""
    SELECT COUNT(*) FROM songs
""")

get_number_artists = ("""
    SELECT COUNT(*) FROM artists
""")

get_number_time = ("""
    SELECT COUNT(*) FROM time
""")



# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, 
                        user_table_create, 
                        song_table_create, 
                        artist_table_create,
                        time_table_create, 
                        songplay_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, 
                      songplay_table_drop, user_table_drop, 
                      song_table_drop, 
                      artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, 
                        song_table_insert, artist_table_insert, 
                        time_table_insert]

select_number_rows_queries= [get_number_staging_events, 
                             get_number_staging_songs, 
                             get_number_songplays, 
                             get_number_users, 
                             get_number_songs, 
                             get_number_artists, 
                             get_number_time]
