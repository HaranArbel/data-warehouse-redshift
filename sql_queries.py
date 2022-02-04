import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_DB = config.get("CLUSTER", "DB_NAME")
DWH_DB_USER = config.get("CLUSTER", "DB_USER")
DWH_DB_PASSWORD = config.get("CLUSTER", "DB_PASSWORD")
DWH_PORT = config.get("CLUSTER", "DB_PORT")

ARN = config.get('IAM_ROLE', 'ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_fact"
user_table_drop = "DROP TABLE IF EXISTS user_dim"
song_table_drop = "DROP TABLE IF EXISTS song_dim"
artist_table_drop = "DROP TABLE IF EXISTS artist_dim"
time_table_drop = "DROP TABLE IF EXISTS time_dim"


# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        event_id        INT IDENTITY(0,1),
        artist          VARCHAR,
        auth            VARCHAR,
        firstName       VARCHAR,
        gender          VARCHAR,
        itemInSession   VARCHAR,
        lastName        VARCHAR,
        length          VARCHAR,
        level           VARCHAR,
        location        VARCHAR,
        method          VARCHAR,
        page            VARCHAR,
        registration    VARCHAR,
        sessionId       INTEGER,
        song            VARCHAR,
        status          INTEGER,
        ts              BIGINT,
        userAgent       VARCHAR,
        userId          INTEGER
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        song_id          VARCHAR,
        num_songs        INT,       
        artist_id        VARCHAR,
        artist_latitude  DECIMAL,
        artist_longitude DECIMAL,
        artist_location  VARCHAR,
        artist_name      VARCHAR,
        title            VARCHAR,
        duration         DECIMAL,
        year             INT
        );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay_fact (
        songplay_id     INT IDENTITY(0,1),
        start_time      TIMESTAMP               sortkey,
        user_id         VARCHAR, 
        level           VARCHAR,
        song_id         VARCHAR                 distkey, 
        artist_id       VARCHAR, 
        session_id      INT, 
        location        VARCHAR, 
        user_agent      VARCHAR
    );
""")

user_table_create = ("""
    CREATE TABLE user_dim(
        user_id         VARCHAR                 sortkey,
        first_name      VARCHAR(255),
        last_name       VARCHAR(255),
        gender          VARCHAR(1),
        level           VARCHAR(50)
    );
""")

song_table_create = ("""
    CREATE TABLE song_dim(
        song_id         VARCHAR(100)            sortkey distkey,
        title           VARCHAR(255),
        artist_id       VARCHAR(100)            NOT NULL,
        year            INTEGER,
        duration        DECIMAL
    );
""")

artist_table_create = ("""
    CREATE TABLE artist_dim(
        artist_id       VARCHAR(100)            sortkey,
        name            VARCHAR(255),
        location        VARCHAR(255),
        latitude        DECIMAL,
        longitude       DECIMAL
    );
""")

time_table_create = ("""
    CREATE TABLE time_dim(
        start_time      TIMESTAMP               sortkey,
        hour            INTEGER,
        day             INTEGER,
        week            INTEGER,
        month           INTEGER,
        year            INTEGER,
        weekday         INTEGER
    );
""")

# STAGING TABLES

staging_songs_copy = ("""
    COPY staging_songs
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    FORMAT AS JSON 'auto' 
    REGION 'us-west-2';
""").format(config.get('S3', 'SONG_DATA'),
            config.get('IAM_ROLE', 'ARN'))

staging_events_copy = ("""
    COPY staging_events 
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    JSON {}
    REGION 'us-west-2';
""").format(config.get('S3', 'LOG_DATA'),
            config.get('IAM_ROLE', 'ARN'),
            config.get('S3', 'LOG_JSONPATH'))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay_fact (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second' AS start_time,              
            e.userId              AS user_id,
            e.level               AS level,
            s.song_id             AS song_id,
            s.artist_id           AS artist_id,
            e.sessionId           AS session_id,
            e.location            AS location,
            e.userAgent           AS user_agent
    FROM staging_events e
    JOIN staging_songs s 
    ON e.song = s.title
    AND e.artist = s.artist_name
    WHERE e.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO user_dim (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId,
                    firstName,
                    lastName,
                    gender,
                    level
    FROM staging_events
    WHERE page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO song_dim (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id,
                    title,
                    artist_id,
                    year,
                    duration
    FROM staging_songs;
""")

artist_table_insert = ("""
    INSERT INTO artist_dim (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id,
                    artist_name,
                    artist_location,
                    artist_latitude,
                    artist_longitude
    FROM staging_songs;
""")

time_table_insert = ("""
    INSERT INTO time_dim(start_time, hour, day, week, month, year, weekDay)
    SELECT start_time, 
        extract(hour from start_time),
        extract(day from start_time),
        extract(week from start_time), 
        extract(month from start_time),
        extract(year from start_time), 
        extract(dayofweek from start_time)
    FROM songplay_fact;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

