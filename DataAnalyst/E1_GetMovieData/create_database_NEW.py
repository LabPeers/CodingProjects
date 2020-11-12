import pandas as pd
import sqlite3
import os
from math import nan
import time

from sqlite_database_schema import build_database


t0 = time.time()



conn = sqlite3.connect('movies_new.db')
c = conn.cursor()

build_database(conn)

#------ Load data
movies = pd.read_csv("../archive/Movie_Movies.csv", delimiter=',',low_memory=False)
actors = pd.read_csv("../archive/Movie_Actors.csv", delimiter=',')
rating = pd.read_csv("../archive/Movie_AdditionalRating.csv", delimiter=',')
genres = pd.read_csv("../archive/Movie_Genres.csv", delimiter=',')
writers = pd.read_csv("../archive/Movie_Writer.csv", delimiter=',')


#------ Data cleaning
#1. Drop unnecessary columns
movies.drop(['DVD','Production','Rated','Released','Runtime'], axis=1, inplace=True)
actors.drop(['Unnamed: 0'],axis=1,inplace=True)
rating.drop(['Unnamed: 0'],axis=1,inplace=True)
genres.drop(['Unnamed: 0'],axis=1,inplace=True)
writers.drop(['Unnamed: 0'],axis=1,inplace=True)

#2. Remove tt from imdbID column and turn into integer
def remove_tt(df):
    df.dropna(subset=['imdbID'],inplace=True)
    df['imdbID']=(df['imdbID'].str[2:]).astype(int) #remove tt from imdbID and turn into integer

remove_tt(movies)
remove_tt(actors)
remove_tt(rating)
remove_tt(genres)
remove_tt(writers)

#3. Remove duplicates
def remove_duplicates(df):
    df.drop_duplicates(keep = False, inplace = True)
    

remove_duplicates(movies)
remove_duplicates(actors)
remove_duplicates(rating)
remove_duplicates(genres)
remove_duplicates(writers)

#4. Remove empty spaces
genres["Genre"]=genres["Genre"].str.replace(" ","")

#5. Unify specific columns
# Unify ratings column
rating['Rating']=rating['Rating'].str.replace("%","")
rating['Rating']=rating['Rating'].str.replace("/100","")
rating['Rating']=rating['Rating'].str.replace("/10","*10")
rating['Rating']=rating['Rating'].apply(lambda x: str(int(eval(x)))+"%")



#------ Prepare movies table:
#Load data into SQL
movies.to_sql('Movies', con=conn, if_exists='append',index=False)
conn.commit()


#------Prepare actors and actors_movies_join tables:
#Make new index for same actor
actors['aID'] = actors.groupby(['Actors']).ngroup() #Number each group

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
rating['rID'] = rating.groupby(['Rating','RatingSource']).ngroup() #Number each group
print(rating.shape)

joint_ratings_movies=rating[['rID', 'imdbID']].copy()
joint_ratings_movies=joint_ratings_movies.drop_duplicates().copy()

joint_ratings_movies.to_sql('Movies_Raitings_Join', con=conn, if_exists='append',index=False)
conn.commit()

rating_new=rating[['rID','Rating','RatingSource']]
rating_new=rating_new.drop_duplicates()
rating_new.to_sql('AdditionalRating', con=conn, if_exists='append',index=False)
conn.commit()


#------Prepare genres and genres_movies_join tables:
genres['gID'] = genres.groupby(['Genre']).ngroup() #Number each group

joint_genres_movies=genres[['gID', 'imdbID']].copy()
joint_genres_movies=joint_genres_movies.drop_duplicates().copy()
joint_genres_movies.to_sql('Movies_Genres_Join', con=conn, if_exists='append',index=False)
conn.commit()

genres=genres[['gID','Genre']].copy()
genres=genres.drop_duplicates().copy()
genres.to_sql('Genres', con=conn, if_exists='append',index=False)
conn.commit()


#------Prepare writers and writers_movies_join tables:
writers['wID'] = writers.groupby(['Person','Responsibility']).ngroup() #Number each group

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



