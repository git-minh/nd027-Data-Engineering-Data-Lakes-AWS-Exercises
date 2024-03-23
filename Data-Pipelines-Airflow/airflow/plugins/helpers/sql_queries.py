class SqlQueries:
    songplay_table_insert = """
        INSERT INTO songplays(
            songplay_id,
            start_time, 
            user_id, 
            level, 
            song_id, 
            artist_id, 
            session_id, 
            location, 
            user_agent)
        SELECT
            md5(events.sessionid || events.start_time) AS songplay_id,
            events.start_time, 
            events.userid, 
            events.level, 
            songs.song_id, 
            songs.artist_id, 
            events.sessionid, 
            events.location, 
            events.useragent
        FROM 
            (SELECT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time, * 
             FROM staging_events
             WHERE page = 'NextSong') events
        LEFT JOIN staging_songs songs
            ON events.song = songs.title
            AND events.artist = songs.artist_name
            AND events.length = songs.duration
    """

    user_table_insert = """
        INSERT INTO users(user_id, first_name, last_name, gender, level)
        SELECT DISTINCT 
            userid AS user_id, 
            firstname AS first_name, 
            lastname AS last_name, 
            gender, 
            level
        FROM staging_events
        WHERE page = 'NextSong'
    """

    song_table_insert = """
        INSERT INTO songs(song_id, title, artist_id, year, duration)
        SELECT DISTINCT 
            song_id, 
            title, 
            artist_id, 
            year, 
            duration
        FROM staging_songs
    """

    artist_table_insert = """
        INSERT INTO artists(artist_id, name, location, latitude, longitude)
        SELECT DISTINCT 
            artist_id, 
            artist_name AS name, 
            artist_location AS location, 
            artist_latitude AS latitude, 
            artist_longitude AS longitude
        FROM staging_songs
    """

    time_table_insert = """
        INSERT INTO time(start_time, hour, day, week, month, year, weekday)
        SELECT 
            start_time, 
            EXTRACT(hour FROM start_time), 
            EXTRACT(day FROM start_time), 
            EXTRACT(week FROM start_time), 
            EXTRACT(month FROM start_time), 
            EXTRACT(year FROM start_time), 
            EXTRACT(dayofweek FROM start_time)
        FROM songplays
    """
