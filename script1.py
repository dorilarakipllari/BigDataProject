from pymongo import MongoClient
import pandas as pd

def load_ratings():
    client = MongoClient("mongodb+srv://drakipllari:admin@bigdatacluster.blg0r1y.mongodb.net/")
    db = client['movielens']
    cursor = db.ratings.find()
    ratings = pd.DataFrame(list(cursor))
    if '_id' in ratings.columns:
        ratings = ratings.drop(columns=['_id'])
    return ratings

def load_movies():
    client = MongoClient("mongodb+srv://drakipllari:admin@bigdatacluster.blg0r1y.mongodb.net/")
    db = client['movielens']
    cursor = db.movies.find()
    movies = pd.DataFrame(list(cursor))
    if '_id' in movies.columns:
        movies = movies.drop(columns=['_id'])
    return movies

def main():
    # Ngarkojmë të dhënat nga MongoDB
    ratings = load_ratings()
    movies = load_movies()

    # Bashkojmë të dhënat për titujt e filmave
    data = pd.merge(ratings, movies[['item_id', 'title']], on='item_id')

    # Llogarit mesataren dhe numrin e vlerësimeve për çdo film
    agg = data.groupby('title')['rating'].agg(['mean', 'count'])

    # Filtron filmat me më shumë se 50 vlerësime
    agg = agg[agg['count'] >= 50]

    # Rendit nga më i larti në më të ulët
    agg = agg.sort_values('mean', ascending=False)

    # Printo top 10 filmat
    print("Top 10 filmat më të vlerësuar (me më shumë se 50 vlerësime):\n")
    print(agg.head(10))

if __name__ == "__main__":
    main()
