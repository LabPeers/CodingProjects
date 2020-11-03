import pandas as pd
from pandas_profiling import ProfileReport



actors=pd.read_csv("archive/Movie_Actors.csv")
print(actors.head())
prof1 = ProfileReport(actors)
prof1.to_file('E1_GetMovieData/actors.html')

ratings=pd.read_csv("archive/Movie_AdditionalRating.csv")
print(ratings.head())
prof2 = ProfileReport(ratings)
prof2.to_file('E1_GetMovieData/ratings.html')

genres=pd.read_csv("archive/Movie_Genres.csv")
print(genres.head())
prof3 = ProfileReport(genres)
prof3.to_file('E1_GetMovieData/genres.html')

movies=pd.read_csv("archive/Movie_Movies.csv")
print(ratings.head())
prof4 = ProfileReport(movies)
prof4.to_file('E1_GetMovieData/movies.html')

writers=pd.read_csv("archive/Movie_Writer.csv")
print(writers.head())
prof5 = ProfileReport(writers)
prof5.to_file('E1_GetMovieData/writers.html')