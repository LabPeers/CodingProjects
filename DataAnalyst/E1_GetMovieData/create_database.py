import pandas as pd
import sqlite3
import os
from math import nan
import time

from sqlite_database_schema import build_database


t0 = time.time()



conn = sqlite3.connect('E1_GetMovieData/movies.db')
c = conn.cursor()

build_database(conn)

#------ Prepare movies table:
movies = pd.read_csv("archive/Movie_Movies.csv", delimiter=',',low_memory=False)
#print(movies[movies['imdbID'].isnull()])#To identify rows with NaN
movies = movies.dropna(subset=['imdbID'])
movies['imdbID']=(movies['imdbID'].str[2:]).astype(int) #remove tt from imdbID and turn into integer
#Load data into SQL
movies.to_sql('Movies', con=conn, if_exists='append',index=False)
conn.commit()


#------Prepare actors and actors_movies_join tables:
#Make new index for same actor
actors = pd.read_csv("archive/Movie_Actors.csv", delimiter=',')
#actors = actors.rename(columns={'Unnamed: 0': 'ID'})
actors['aID'] = actors.groupby(['Actors']).ngroup() #Number each group
actors['imdbID']=(actors['imdbID'].str[2:]).astype(int) #remove tt from imdbID and turn into integer

joint_actors_movies=actors[['aID', 'imdbID']].copy()
joint_actors_movies.to_sql('Movies_Actors_Join', con=conn, if_exists='append',index=False)
conn.commit()

actors_new=actors[['aID','Actors']]
#print(actors_new.size)
actors_new=actors_new.drop_duplicates()
#print(actors_new.size)
actors_new.to_sql('Actors', con=conn, if_exists='append',index=False)
conn.commit()


#------Prepare ratings and ratings_movies_join tables:

rating = pd.read_csv("archive/Movie_AdditionalRating.csv", delimiter=',')
rating = rating.rename(columns={'Unnamed: 0': 'ID'})
rating['rID'] = rating.groupby(['Rating','RatingSource']).ngroup() #Number each group
print(rating.shape)
rating['imdbID']=(rating['imdbID'].str[2:]).astype(int) #remove tt from imdbID and turn into integer

joint_ratings_movies=rating[['rID', 'imdbID']].copy()
joint_ratings_movies=joint_ratings_movies.drop_duplicates().copy()

joint_ratings_movies.to_sql('Movies_Raitings_Join', con=conn, if_exists='append',index=False)
conn.commit()

rating_new=rating[['rID','Rating','RatingSource']]
rating_new=rating_new.drop_duplicates()
rating_new.to_sql('AdditionalRating', con=conn, if_exists='append',index=False)
conn.commit()


#------Prepare genres and genres_movies_join tables:
genres = pd.read_csv("archive/Movie_Genres.csv", delimiter=',')
genres = genres.rename(columns={'Unnamed: 0': 'ID'})
genres['gID'] = genres.groupby(['Genre']).ngroup() #Number each group
genres['imdbID']=(genres['imdbID'].str[2:]).astype(int) #remove tt from imdbID and turn into integer

joint_genres_movies=genres[['gID', 'imdbID']].copy()
joint_genres_movies=joint_genres_movies.drop_duplicates().copy()
joint_genres_movies.to_sql('Movies_Genres_Join', con=conn, if_exists='append',index=False)
conn.commit()

genres=genres[['gID','Genre']].copy()
genres=genres.drop_duplicates().copy()
genres.to_sql('Genres', con=conn, if_exists='append',index=False)
conn.commit()


#------Prepare writers and writers_movies_join tables:
writers = pd.read_csv("archive/Movie_Writer.csv", delimiter=',')
writers = writers.rename(columns={'Unnamed: 0': 'ID'})
writers['wID'] = writers.groupby(['Person','Responsibility']).ngroup() #Number each group
writers['imdbID']=(writers['imdbID'].str[2:]).astype(int) #remove tt from imdbID and turn into integer

joint_writers_movies=writers[['wID', 'imdbID']].copy()
joint_writers_movies=joint_genres_movies.drop_duplicates().copy()
joint_writers_movies.to_sql('Movies_Writers_Join', con=conn, if_exists='append',index=False)
conn.commit()

writers=writers[['wID','Person','Responsibility']].copy()
writers=writers.drop_duplicates().copy()
writers.to_sql('Writer', con=conn, if_exists='append',index=False)
conn.commit()



conn.close()

t1=time.time()
print("took {} s".format(t1-t0))



