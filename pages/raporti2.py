import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pymongo import MongoClient

@st.cache_data
def load_data():
    mongo_uri = "mongodb+srv://drakipllari:admin@bigdatacluster.blg0r1y.mongodb.net/"
    client = MongoClient(mongo_uri)
    db = client["movielens"]

    ratings = pd.DataFrame(list(db.ratings.find()))
    users = pd.DataFrame(list(db.users.find()))
    movies = pd.DataFrame(list(db.movies.find()))

    # Heq kolonën _id
    for df in [ratings, users, movies]:
        if '_id' in df.columns:
            df.drop(columns=['_id'], inplace=True)

    movies['release_year'] = pd.to_datetime(movies['release_date'], errors='coerce').dt.year

    data = pd.merge(ratings, users[['user_id', 'age']], on='user_id')
    data = pd.merge(data, movies[['item_id', 'release_year']], on='item_id')
    data = data.dropna(subset=['release_year', 'age'])

    bins = [7, 17, 27, 37, 47, 57, 100]
    labels = ['7-17', '18-27', '28-37', '38-47', '48-57', '58+']
    data['age_group'] = pd.cut(data['age'], bins=bins, labels=labels, right=True, include_lowest=True)

    return data

def main():
    st.title("Mesatarja e vlerësimeve sipas grupmoshave dhe viteve të filmave")

    data = load_data()

    agg = data.groupby(['age_group', 'release_year'], observed=True)['rating'].mean().reset_index()

    min_year = int(data['release_year'].min())
    max_year = int(data['release_year'].max())
    year_range = st.slider("Zgjidh gamën e viteve të filmave:", min_year, max_year, (1980, 2000))

    filtered = agg[(agg['release_year'] >= year_range[0]) & (agg['release_year'] <= year_range[1])]

    pivot = filtered.pivot(index='age_group', columns='release_year', values='rating')

    st.write("Mesatarja e vlerësimeve për grupmosha dhe vite filmash:")

    fig, ax = plt.subplots(figsize=(12, 6))
    sns.heatmap(pivot, annot=True, fmt=".2f", cmap="YlGnBu", ax=ax)
    st.pyplot(fig)

if __name__ == "__main__":
    main()
