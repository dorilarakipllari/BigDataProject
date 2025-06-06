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

def load_users():
    client = MongoClient("mongodb+srv://drakipllari:admin@bigdatacluster.blg0r1y.mongodb.net/")
    db = client['movielens']
    cursor = db.users.find()
    users = pd.DataFrame(list(cursor))
    if '_id' in users.columns:
        users = users.drop(columns=['_id'])
    return users

def load_movies():
    client = MongoClient("mongodb+srv://drakipllari:admin@bigdatacluster.blg0r1y.mongodb.net/")
    db = client['movielens']
    cursor = db.movies.find()
    movies = pd.DataFrame(list(cursor))
    if '_id' in movies.columns:
        movies = movies.drop(columns=['_id'])
    return movies

def main():
    ratings = load_ratings()
    users = load_users()
    movies = load_movies()

    movies['release_year'] = pd.to_datetime(movies['release_date'], errors='coerce').dt.year

    data = pd.merge(ratings, users[['user_id', 'age']], on='user_id')
    data = pd.merge(data, movies[['item_id', 'release_year']], on='item_id')

    data = data.dropna(subset=['release_year', 'age'])

    bins = [7, 17, 27, 37, 47, 57, 100]
    labels = ['7-17', '18-27', '28-37', '38-47', '48-57', '58+']
    data['age_group'] = pd.cut(data['age'], bins=bins, labels=labels, right=True, include_lowest=True)

    agg = data.groupby(['age_group', 'release_year'], observed=True)['rating'].mean().reset_index()
    max_ratings = agg.loc[agg.groupby('age_group', observed=True)['rating'].idxmax()]

    for idx, row in max_ratings.iterrows():
        print(f"Përdoruesit në moshën {row['age_group']} kanë dhënë vlerësimin maksimal për vitin {int(row['release_year'])} me mesataren {row['rating']:.2f}")

if __name__ == "__main__":
    main()
