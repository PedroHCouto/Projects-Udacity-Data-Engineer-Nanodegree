import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS events_table;"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs_table;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table;"
user_table_drop = "DROP TABLE IF EXISTS user_table;"
song_table_drop = "DROP TABLE IF EXISTS song_table;"
artist_table_drop = "DROP TABLE IF EXISTS artist_table;"
time_table_drop = "DROP TABLE IF EXISTS time_table;"

# CREATE EVENTS TABLES FOR STAGING

staging_events_table_create= ("""CREATE TABLE staging_events_table (
    artist              VARCHAR(55)     NOT NULL,
    auth                VARCHAR(15)     NOT NULL,
    first_name          VARCHAR(55)     NOT NULL,
    gender              VARCHAR(1)      NOT NULL,
    item_in_session     INTEGER         NOT NULL,
    last_name           VARCHAR(55)     NOT NULL,
    length              NUMERIC         NOT NULL,
    level               VARCHAR(4)      NOT NULL,
    location            TEXT            NOT NULL,
    method              VARCHAR(3)      NOT NULL,
    page                VARCHAR(8)      NOT NULL,
    registration        NUMERIC         NOT NULL,
    session_id          INTEGER         NOT NULL,
    song                TEXT            NOT NULL,
    status              INTEGER         NOT NULL,
    ts                  BIGINT          NOT NULL,
    user_agent          TEXT            NOT NULL,
    user_id             INTEGER         NOT NULL,
    );""")

staging_songs_table_create = ("""CREATE TABLE staging_songs_table (
    num_songs           INTEGER         NOT NULL,
    artist_id           VARCHAR(55)     NOT NULL,
    artist_latitude     VARCHAR(55),
    artist_longitude    VARCHAR(55),
    artist_location     VARCHAR(55)     NOT NULL,
    artist_name         VARCHAR(55)     NOT NULL,
    song_id             VARCHAR(55)     NOT NULL,
    title               VARCHAR(155)    NOT NULL,
    duration            NUMERIC         NOT NULL,
    year                INTEGER         NOT NULL
    );""")

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay_table 
    (songplay_id    INTEGER        IDENTITY(1, 1)       distkey,
    start_time      TIMESTAMP      NOT NULL,
    user_id         INTEGER        NOT NULL,
    level           VARCHAR(4)     NOT NULL,
    song_id         INTEGER        NOT NULL,
    artist_id       INTEGER        NOT NULL,
    session_id      INTEGER        NOT NULL,
    location        TEXT           NOT NULL,
    user_agent      TEXT           NOT NULL,
    );""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS user_table
    (user_id        INTEGER        NOT NULL sortkey,
    first_name      VARCHAR(55)    NOT NULL,
    last_name       VARCHAR(55)    NOT NULL,
    gender          VARCHAR(1)     NOT NULL,
    level           VARCHAR(4)     NOT NULL,)
diststyle all;""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS song_table
    (song_id        INTEGER        NOT NULL sortkey,
    title           TEXT           NOT NULL,
    artist_id       INTEGER        NOT NULL,
    year            INTEGER        NOT NULL,
    duration        NUMERIC        NOT NULL)
diststyle all;""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist_id
    (artist_id      INTEGER        NOT NULL sortkey,
    name            TEXT           NOT NULL,
    location        TEXT           NOT NULL,
    lattitude       VARCHAR(55)    NOT NULL,
    longitude       VARCHAR(55)    NOT NULL,)
diststyle all;""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time_table
    (time_id        INTEGER        IDENTITY(1, 1) distkey,
    start_time      TIMESTAMP      NOT NULL,
    hour            TIMESTAMP      NOT NULL,
    day             TIMESTAMP      NOT NULL,
    week            TIMESTAMP      NOT NULL,
    month           TIMESTAMP      NOT NULL,
    year            TIMESTAMP      NOT NULL,
    weekday         TIMESTAMP      NOT NULL) """)

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events_table
    FROM {}
    CREDENTIALS 'aws_arn_role = {}'
    gzip
    REGION {}
""").format()

staging_songs_copy = ("""
    COPY staging_songs_table

""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
