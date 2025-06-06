import streamlit as st
import pandas as pd
from pymongo import MongoClient

@st.cache_data
def load_data():
    mongo_uri = "mongodb+srv://drakipllari:admin@bigdatacluster.blg0r1y.mongodb.net/"
    client = MongoClient(mongo_uri)
    db = client["movielens"]

    ratings = pd.DataFrame(list(db.ratings.find()))
    movies = pd.DataFrame(list(db.movies.find()))

    # Heq kolonën _id që Mongo krijon automatikisht
    if '_id' in ratings.columns:
        ratings = ratings.drop(columns=['_id'])
    if '_id' in movies.columns:
        movies = movies.drop(columns=['_id'])

    data = pd.merge(ratings, movies[['item_id', 'title']], on='item_id')
    return data

def main():
    st.title("Top Filmat më të Vlerësuar")

    data = load_data()
    agg = data.groupby('title')['rating'].agg(['mean', 'count'])
    agg = agg[agg['count'] >= 50]
    agg = agg.sort_values('mean', ascending=False)

    n = st.slider("Sa filma të shfaqen?", 5, 20, 10)
    top_films = agg.head(n)

    st.bar_chart(top_films['mean'])
    st.write(top_films)

if __name__ == "__main__":
    main()
