class CreateTable():
    # create staging tables
    create_staging_events_table = ("""
        CREATE TABLE IF NOT EXISTS {}.staging_events (
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

    create_staging_songs_table = ("""
        CREATE TABLE IF NOT EXISTS {}.staging_songs (
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


    # create fact and dimension tables
    songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS {}.songplays (
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
    CREATE TABLE IF NOT EXISTS {}.{} (
        user_id         VARCHAR        NOT NULL PRIMARY KEY,
        first_name      VARCHAR,
        last_name       VARCHAR,
        gender          CHAR,
        level           VARCHAR(4)     NOT NULL)
    diststyle all;
    """)

    song_table_create = ("""
    CREATE TABLE IF NOT EXISTS {}.{} (
        song_id         VARCHAR        NOT NULL PRIMARY KEY sortkey,
        title           TEXT           NOT NULL,
        artist_id       VARCHAR        NOT NULL,
        year            INTEGER        NOT NULL,
        duration        FLOAT          NOT NULL)
    diststyle all;
    """)

    artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS {}.{} (
        artist_id       VARCHAR        NOT NULL PRIMARY KEY sortkey,
        name            TEXT           NOT NULL,
        location        VARCHAR,
        latitude        VARCHAR,
        longitude       VARCHAR)
    diststyle all;
    """)

    time_table_create = ("""
    CREATE TABLE IF NOT EXISTS {}.{} (
        time_id         INTEGER        IDENTITY(1, 1) PRIMARY KEY distkey,
        start_time      TIMESTAMP      NOT NULL,
        hour            INTEGER        NOT NULL,
        day             INTEGER        NOT NULL,
        week            INTEGER        NOT NULL,
        month           INTEGER        NOT NULL,
        year            INTEGER        NOT NULL,
        weekday         INTEGER        NOT NULL);
    """)    
