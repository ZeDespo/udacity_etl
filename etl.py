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


def parse_and_insert(cur: psycopg2, conn: psycopg2.connect) -> None:
    """
    Take the data in the staging tables and distribute them into their proper Redshift analytic tables.
    :param cur: PostgreSQL cursor
    :param conn: PostgreSQL connection object
    :return:
    """
    logger.info("Fast inserting records into songs table.")
    cur.execute(song_table_fast_insert)
    logger.info("Fast inserting records into artists table.")
    cur.execute(artist_table_fast_insert)
    conn.commit()
    cur.execute(events_select)
    data = cur.fetchall()
    columns = ['user_id', 'first_name', 'last_name', 'gender', 'level', 'ts', 'artist_name', 'song_name', 'length',
               'location', 'session_id', 'user_agent']
    df = pd.DataFrame(data, columns=columns)
    logger.info("Slow inserting records into time, users, and songplay tables.")
    _insert_df_to_time_user_songplay(cur, df)
    conn.commit()


def _insert_df_to_time_user_songplay(cur, df: pd.DataFrame) -> None:
    """
    Take the data from the events' staging table and format it to the time, users, and songplay fact table respectively
    Instead of continuously reading from the database to locate the song id and artist id without use of a primary key,
    another read from the staging table is loaded into a pandas dataframe for in-memory speeds.
    :param cur: PostgreSQL cursor
    :param df: The events dataframe that holds relevant data for the users, time and songplay tables
    :param songs_df: The dataframe representing the songs library necessary to lookup song_id and artist_id for the
                        songplay fact table.
    :return: Nothing
    """
    df = df.drop_duplicates(keep='last')
    df = _timestamps_to_datetime(df)
    cur.execute(artists_select)
    songs_df = pd.DataFrame(cur.fetchall(), columns=['song_id', 'song_name', 'artist_id', 'artist_name'])
    t_c = ["start_time", "hour", "day", "week", "month", "year", "weekday"]
    u_c = ['user_id', 'first_name', 'last_name', 'gender', 'level']
    s_c = ['start_time', 'user_id', 'level', 'session_id', 'location', 'user_agent', 'song_id', 'artist_id']
    df['song_id'], df['artist_id'] = None, None
    logger.debug("Inserting {} rows".format(len(df)))
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
