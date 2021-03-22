import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Process the song data file and insert the data into artists and songs tables. 

    Args:
        cur: psycopg2 cursor created from the connection with the DB;
        filepath: root directory where the other directories with the song data files are.
    """
    # open song file
    df = pd.read_json(filepath, lines = True)

    # insert artist record
    artist_data = df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)

    # insert song record
    song_data = df[["song_id", "title", "artist_id", "year", "duration"]].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    


def process_log_file(cur, filepath):
    """
    Preprocess the log data in 3 steps and insert the data into time, users and songplays tables.
    The steps are:
    1. Extract the ts data, transform it into datetime format and extract "hour", 
    "day", "week", "month", "year", "weekday" features for time table;
    2. Extact the "stat_time", "hour", "day", "week", "month", "year", "weekday" features for users table;
    3. Use the song, artist and length data to get artist_id and song_id by querying it from the DB and 
    insert it with the other elements of data into songplays table.

    Args:
        cur: psycopg2 cursor created from the connection with the DB;
        filepath: root directory where the other directories with the log data files are.
    """

    # open log file
    df = pd.read_json(filepath, lines = True)

    # filter by NextSong action
    df = df[df.page == "NextSong"]

    # convert timestamp column to timestamp
    t = pd.to_datetime(df.ts, unit = 'ms',) 
    
    # insert time data records
    time_data = zip(t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ["stat_time", "hour", "day", "week", "month", "year", "weekday"]
    time_df = pd.DataFrame(time_data, columns = column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId","firstName", "lastName", "gender", "level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get song_id and artist_id from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (index, pd.to_datetime(row.ts, unit = 'ms'), row.userId, 
                        row.level, songid, artistid, 
                        row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Extract all paths of the files stored in the parsed filepath, 
    apply the parsed function to each datafile commiting the operation 
    at the end of each step.  

    Args:
        cur: psycopg2 cursor created from the connection with the DB;
        conn: connection with the DB established through psycopg2;
        filepath: root directory where the other directories with the log data files are;
        func: one of the two functions created above - process_log_file() or process_log_file()

    """

    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Establish the connection, create a cursor and process the data calling first the process_data 
    function with process_song_file and them with process_log_file as input.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()