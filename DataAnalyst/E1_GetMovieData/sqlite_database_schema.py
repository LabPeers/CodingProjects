import sqlite3


def execute_query(query,conn):
    c = conn.cursor()
    
    c.execute(query)
    conn.commit()
    print("Query executed successfully")



def build_database(conn):

    #Create Database Tables:

    #1. Movies Table:
    create_movies_table = """
    CREATE TABLE IF NOT EXISTS Movies (
    awards TEXT,
    country TEXT,
    DVD TEXT,
    director TEXT,
    language TEXT,
    plot TEXT,
    poster TEXT,
    production TEXT,
    rated TEXT,
    released TEXT,
    runtime TEXT,
    title TEXT,
    type TEXT,
    website TEXT,
    year INTEGER,
    imdbID INTEGER PRIMARY KEY,
    imdbRating NUMERIC,
    imdbVotes INTEGER
    );
    """
    execute_query(create_movies_table,conn) 


    #2. Actors Table:
    create_actors_table = """
    CREATE TABLE IF NOT EXISTS Actors (
    aID INTEGER PRIMARY KEY,
    actors TEXT
    );
    """
    execute_query(create_actors_table,conn) 

    #3. Join Actors and Movies Table:
    create_movies_actors_join_table = """
    CREATE TABLE IF NOT EXISTS Movies_Actors_Join (
    aID INTEGER,
    imdbID INTEGER,
    PRIMARY KEY (aID, imdbID)
    FOREIGN KEY (aID) REFERENCES Actors (aID)
    ON DELETE CASCADE ON UPDATE CASCADE, 
    FOREIGN KEY (imdbID) REFERENCES Movies (imdbID)
    ON DELETE CASCADE ON UPDATE CASCADE
    );
    """
    execute_query(create_movies_actors_join_table,conn) 


    #4. Rating Table:
    create_ratings_table = """
    CREATE TABLE IF NOT EXISTS AdditionalRating (
    rID INTEGER,
    rating TEXT,
    ratingSource TEXT
    );
    """
    execute_query(create_ratings_table,conn)

    
    #5. Join Ratings and Movies Table:
    create_movies_raitings_join_table = """
    CREATE TABLE IF NOT EXISTS Movies_Raitings_Join (
    rID INTEGER,
    imdbID INTEGER,
    PRIMARY KEY (rID, imdbID)
    FOREIGN KEY (rID) REFERENCES Actors (rID)
    ON DELETE CASCADE ON UPDATE CASCADE, 
    FOREIGN KEY (imdbID) REFERENCES Movies (imdbID)
    ON DELETE CASCADE ON UPDATE CASCADE
    );
    """
    execute_query(create_movies_raitings_join_table,conn) 


    #6. Genres Table:
    create_genres_table = """
    CREATE TABLE IF NOT EXISTS Genres (
    gID INTEGER PRIMARY KEY,
    genre TEXT
    );
    """
    execute_query(create_genres_table,conn)

    
    #7. Join Genres and Movies Table:
    create_movies_genres_join_table = """
    CREATE TABLE IF NOT EXISTS Movies_Genres_Join (
    gID INTEGER,
    imdbID INTEGER,
    PRIMARY KEY (gID, imdbID)
    FOREIGN KEY (gID) REFERENCES Genres (gID)
    ON DELETE CASCADE ON UPDATE CASCADE, 
    FOREIGN KEY (imdbID) REFERENCES Movies (imdbID)
    ON DELETE CASCADE ON UPDATE CASCADE
    );
    """
    execute_query(create_movies_genres_join_table,conn) 


    #8. Writer Table:
    create_writer_table = """
    CREATE TABLE IF NOT EXISTS Writer (
    wID INTEGER PRIMARY KEY,
    Person TEXT,
    Responsibility TEXT
    );
    """
    #execute_query(create_writer_table,conn)

    
    #9. Join Writers and Movies Table:
    create_movies_writers_join_table = """
    CREATE TABLE IF NOT EXISTS Movies_Writers_Join (
    wID INTEGER,
    imdbID INTEGER,
    PRIMARY KEY (wID, imdbID)
    FOREIGN KEY (wID) REFERENCES Actors (wID)
    ON DELETE CASCADE ON UPDATE CASCADE, 
    FOREIGN KEY (imdbID) REFERENCES Movies (imdbID)
    ON DELETE CASCADE ON UPDATE CASCADE
    );
    """
    execute_query(create_movies_actors_join_table,conn) 
