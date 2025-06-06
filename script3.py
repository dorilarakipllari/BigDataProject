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

    # Bashko ratings me users për gjininë
    data = pd.merge(ratings, users[['user_id', 'gender']], on='user_id')

    # Bashko me movie titujt
    data = pd.merge(data, movies[['item_id', 'title']], on='item_id')

    # Llogarit mesataren dhe numrin e vlerësimeve për secilin film dhe gjini
    agg = data.groupby(['gender', 'title']).agg(avg_rating=('rating', 'mean'), num_ratings=('rating', 'count')).reset_index()

    # Filtron filmat me më shumë se 20 vlerësime
    agg = agg[agg['num_ratings'] >= 20]

    # Për çdo gjini nxjerr 5 filmat më të mirë
    top_male = agg[agg['gender'] == 'M'].sort_values('avg_rating', ascending=False).head(5)
    top_female = agg[agg['gender'] == 'F'].sort_values('avg_rating', ascending=False).head(5)

    print("Top 5 filmat më të preferuar nga meshkujt:\n")
    for i, row in top_male.iterrows():
        print(f"{row['title']:<50} | Mesatarja: {row['avg_rating']:.2f} | Numri i vlerësimeve: {row['num_ratings']}")

    print("\nTop 5 filmat më të preferuar nga femrat:\n")
    for i, row in top_female.iterrows():
        print(f"{row['title']:<50} | Mesatarja: {row['avg_rating']:.2f} | Numri i vlerësimeve: {row['num_ratings']}")

if __name__ == "__main__":
    main()
