import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table;"
user_table_drop = "DROP TABLE IF EXISTS user_table;"
song_table_drop = "DROP TABLE IF EXISTS song_table;"
artist_table_drop = "DROP TABLE IF EXISTS artist_table;"
time_table_drop = "DROP TABLE IF EXISTS time_table;"

# CREATE EVENTS TABLES FOR STAGING

staging_events_table_create= ("""
CREATE TABLE staging_events_table (
    artist              VARCHAR,
    auth                VARCHAR,
    first_name          VARCHAR,
    gender              CHAR,
    item_in_session     INTEGER,
    last_name           VARCHAR,
    length              FLOAT,
    level               VARCHAR,
    location            TEXT,
    method              VARCHAR,
    page                VARCHAR,
    registration        FLOAT,
    session_id          INTEGER,  
    song                TEXT,
    status              INTEGER,
    ts                  BIGINT, 
    user_agent          TEXT,
    user_id             VARCHAR);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs_table (
    num_songs           INTEGER,
    artist_id           VARCHAR,
    artist_latitude     VARCHAR,
    artist_longitude    VARCHAR,
    artist_location     VARCHAR,
    artist_name         TEXT,
    song_id             VARCHAR,
    title               TEXT,
    duration            FLOAT,
    year                INTEGER);
""")

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay_table (
    songplay_id     INTEGER        IDENTITY(0, 1) NOT NULL PRIMARY KEY distkey,
    start_time      TIMESTAMP      NOT NULL,
    user_id         VARCHAR        NOT NULL,
    level           VARCHAR(4)     NOT NULL,
    song_id         VARCHAR(55)    NOT NULL,
    artist_id       VARCHAR(55)    NOT NULL,
    session_id      VARCHAR(55)    NOT NULL,
    location        TEXT           NOT NULL,
    user_agent      TEXT           NOT NULL);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS user_table (
    user_id         VARCHAR        NOT NULL PRIMARY KEY,
    first_name      VARCHAR,
    last_name       VARCHAR,
    gender          CHAR,
    level           VARCHAR(4)     NOT NULL)
diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song_table (
    song_id         VARCHAR        NOT NULL PRIMARY KEY sortkey,
    title           TEXT           NOT NULL,
    artist_id       VARCHAR        NOT NULL,
    year            INTEGER        NOT NULL,
    duration        FLOAT          NOT NULL)
diststyle all;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist_table (
    artist_id       VARCHAR        NOT NULL PRIMARY KEY sortkey,
    name            TEXT           NOT NULL,
    location        VARCHAR,
    latitude        VARCHAR,
    longitude       VARCHAR)
diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time_table (
    time_id         INTEGER        IDENTITY(1, 1) PRIMARY KEY distkey,
    start_time      TIMESTAMP      NOT NULL,
    hour            INTEGER        NOT NULL,
    day             INTEGER        NOT NULL,
    week            INTEGER        NOT NULL,
    month           INTEGER        NOT NULL,
    year            INTEGER        NOT NULL,
    weekday         INTEGER        NOT NULL);
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events_table
    FROM {}
    IAM_ROLE {}
    JSON {}
""").format(
    config.get('S3', 'LOG_DATA'),
    config.get('IAM_ROLE', 'ARN'),
    config.get('S3', 'LOG_JSONPATH')
)

staging_songs_copy = ("""
    COPY staging_songs_table
    FROM {}
    IAM_ROLE {}
    FORMAT AS JSON 'auto';
""").format(
    config.get('S3', 'SONG_DATA'),
    config.get('IAM_ROLE', 'ARN')
)

# FINAL TABLES 

# got the timestamp trick from here: https://stackoverflow.com/questions/39815425/how-to-convert-epoch-to-datetime-redshift
# ts need to be divided over 1000 to move from ms to s.
songplay_table_insert = ("""
    INSERT INTO songplay_table (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
    SELECT TIMESTAMP 'epoch' + (ts / 1000) * interval '1 second',
           user_id,
           level,
           song_id,
           artist_id,
           session_id,
           location,
           user_agent
    FROM staging_events_table e
    JOIN staging_songs_table s ON (e.song = s.title AND e.artist = s.artist_name AND e.length = s.duration)                            
""")

user_table_insert = ("""
INSERT INTO user_table (user_id, first_name, last_name, gender, level)
SELECT DISTINCT (user_id),
       first_name,
       last_name,
       gender,
       level
FROM staging_events_table; 
""")

song_table_insert = ("""
INSERT INTO song_table (song_id, title, artist_id, year, duration)
SELECT DISTINCT (song_id),
       title,
       artist_id,
       year, 
       duration
FROM staging_songs_table
""")

artist_table_insert = ("""
INSERT INTO artist_table (artist_id, name, location, latitude, longitude)
SELECT DISTINCT (artist_id),
       artist_name,
       artist_location,
       artist_latitude,
       artist_longitude
FROM staging_songs_table
""")

# once this table has all its values based on start_time, we can avoid another conversion
# by pulling the data not from staging_events_table but songplay_table.
time_table_insert = ("""
INSERT INTO time_table (start_time, hour, day, week, month, year, weekday)
SELECT start_time,
       EXTRACT(hour FROM start_time),
       EXTRACT(day FROM start_time),
       EXTRACT(week FROM start_time),
       EXTRACT(month FROM start_time),
       EXTRACT(year FROM start_time),
       EXTRACT(weekday FROM start_time)
FROM songplay_table
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create,
                        staging_songs_table_create, 
                        songplay_table_create, 
                        user_table_create, 
                        song_table_create, 
                        artist_table_create, 
                        time_table_create]

drop_table_queries = [staging_events_table_drop, 
                     staging_songs_table_drop, 
                     songplay_table_drop, 
                     user_table_drop, 
                     song_table_drop, 
                     artist_table_drop, 
                     time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert,
                        user_table_insert,
                        song_table_insert, 
                        artist_table_insert,
                        time_table_insert]
