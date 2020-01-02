import logging
import os
import psycopg2
import pandas as pd
from typing import Optional
from sql_queries import *


def create_logger(debug_mode: Optional[bool]=False) -> logging.getLogger:
    """
    Self-explanatory, create a logger for streaming output
    :param debug_mode: Is the developer debugging this or no?
    :return: The logging object.
    """
    logger = logging.getLogger(os.path.basename(__name__))
    logger.setLevel(logging.INFO if not debug_mode else logging.DEBUG)
    formatter = logging.Formatter('%(filename)s:%(funcName)s:%(levelname)s:%(message)s')
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    return logger


def copy_s3_to_staging(cur: psycopg2, conn: psycopg2.connect) -> None:
    """
    Copy the contents of the S3 buckets to the staging tables in the database
    :param cur: PostgreSQL cursor
    :param conn: PostgreSQL connection object
    :return: None
    """
    for query in copy_table_queries:
        logger.debug("Copying data to staging table as\n{}".format(query))
        cur.execute(query)
        conn.commit()


def create_staging_tables(cur: psycopg2, conn: psycopg2.connect) -> None:
    """
    Create the temporary tables used for staging.
    :param cur: PostgreSQL cursor
    :param conn: PostgreSQL connection object
    :return: None
    """
    for query in insert_temp_table_queries:
        logger.debug("Creating staging table as \n{}".format(query))
        cur.execute(query)
    conn.commit()


def parse_and_insert(cur: psycopg2, conn: psycopg2.connect):
    """
    Take the data in the staging tables and distribute them into their proper Redshift analytic tables.
    :param cur: PostgreSQL cursor
    :param conn: PostgreSQL connection object
    :return:
    """
    logger.info("Fast inserting records into song table.")
    cur.execute(song_table_fast_insert)
    cur.execute(artists_select)
    data = cur.fetchall()
    columns = ["song_id", "song_name", "length", "artist_id", "artist_name", "artist_location", "artist_latitude",
               "artist_longitude"]
    songs_df = pd.DataFrame(data, columns=columns)
    logger.info("Slow inserting records into artists table")
    _insert_df_to_artist(cur, songs_df.filter(columns[3:]))
    conn.commit()
    to_drop = ['length', 'artist_location', 'artist_latitude', 'artist_longitude']
    songs_df = songs_df.drop(to_drop, axis=1)  # Just need song_id, song_name, length and artist_id for fact table
    songs_df = songs_df.drop_duplicates(keep='first')
    cur.execute(events_select)
    data = cur.fetchall()
    columns = ['user_id', 'first_name', 'last_name', 'gender', 'level', 'ts', 'artist_name', 'song_name', 'length',
               'location', 'session_id', 'user_agent']
    df = pd.DataFrame(data, columns=columns)
    logger.info("Slow inserting records into time, users, and songplay tables.")
    _insert_df_to_time_user_songplay(cur, df, songs_df)
    conn.commit()


def _insert_df_to_artist(cur, df: pd.DataFrame) -> None:
    """
    Remove duplicates from the dataframe representing the songs' staging table and insert the records into the artist
    table.
    :param cur: PostgreSQL cursor
    :param df: The pandas songs dataframe that holds only the values the artist table concerns itself with.
    :return: None
    """
    a = df.drop_duplicates(keep='last')
    logger.info("Inserting {} rows".format(len(a)))
    for index, row in a.iterrows():
        cur.execute(artist_table_insert, row.to_list())
        if index % 1000 == 0:
            logger.info("{} rows inserted".format(index + 1))
    logger.info("Finished insertion.")


def _insert_df_to_time_user_songplay(cur, df: pd.DataFrame, songs_df: pd.DataFrame) -> None:
    """
    Take the data from the events' staging table and format it to the time, users, and songplay fact table respectively
    :param cur: PostgreSQL cursor
    :param df: The events dataframe that holds relevant data for the users, time and songplay tables
    :param songs_df: The dataframe representing the songs library necessary to lookup song_id and artist_id for the
                        songplay fact table.
    :return: Nothing
    """
    df = df.drop_duplicates(keep='last')
    df = _timestamps_to_datetime(df)
    t_c = ["start_time", "hour", "day", "week", "month", "year", "weekday"]
    u_c =['user_id', 'first_name', 'last_name', 'gender', 'level']
    s_c = ['start_time', 'user_id', 'level', 'session_id', 'location', 'user_agent', 'song_id', 'artist_id']
    df['song_id'], df['artist_id'] = None, None
    logger.info("Inserting {} rows".format(len(df)))
    for index, row in df.iterrows():
        cur.execute(time_table_insert, row.filter(t_c).to_list())
        cur.execute(user_table_insert, row.filter(u_c).to_list())
        song_name, artist_name = row['song_name'], row['artist_name']
        s_df = songs_df.loc[(songs_df['song_name'] == song_name) & (songs_df['artist_name'] == artist_name)]
        if not s_df.empty:
            row.song_id, row.artist_id = s_df['song_id'].iloc[0], s_df['artist_id'].iloc[0]
        row = row.filter(s_c).to_list()
        cur.execute(songplay_table_insert, row)
        if index % 1000 == 0:
            logger.info("{} rows inserted".format(index + 1))
    logger.info("Finished insertion.")


def _timestamps_to_datetime(df: pd.DataFrame) -> pd.DataFrame:
    """
    Breakdown the timestamp into a date / time units and add them to the original data frame passed as a parameter
    :param df: The dataframe to work with
    :return: A newly merged pandas data frame.
    """
    t = pd.to_datetime(df['ts'], unit='ms')
    column_labels = ("start_time", "hour", "day", "week", "month", "year", "weekday",)
    t = t.apply(lambda x: pd.Series([x, x.hour, x.day, x.week, x.month, x.year, x.day_name()],
                                    index=column_labels))
    return pd.concat([df, t], axis=1)


def main():
    """
    Responsible for all ETL operations
    1) Extract the JSON logs from the specified S3 bucket and copy it into Redshift staging tables.
    2) Clean the data and place it in the following Redshift analytical tables:
        Fact:           songplays (songplay_id [PK], start_time, user_id, level, song_id, artist_id, session_id,
                                    location, user_agent)
        Dimensional:    songs (song_id [PK], title, artist_id, year, duration)
                        artists (artist_id [PK], name, location, latitude, longitude)
                        users (user_id [PK], first_name, last_name, gender, level)
                        time (start_time [PK], hour, day, week, month, year, weekday)
    :return:
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    create_staging_tables(cur, conn)
    copy_s3_to_staging(cur, conn)
    parse_and_insert(cur, conn)
    conn.close()


if __name__ == "__main__":
    logger = create_logger(True)
    main()
