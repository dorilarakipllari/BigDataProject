import streamlit as st
import pandas as pd
import altair as alt
from pymongo import MongoClient

@st.cache_data
def load_data():
    mongo_uri = "mongodb+srv://drakipllari:admin@bigdatacluster.blg0r1y.mongodb.net/"
    client = MongoClient(mongo_uri)
    db = client["movielens"]

    ratings = pd.DataFrame(list(db.ratings.find()))
    users = pd.DataFrame(list(db.users.find()))
    movies = pd.DataFrame(list(db.movies.find()))

    # Heq kolonën _id që Mongo krijon automatikisht
    for df in [ratings, users, movies]:
        if '_id' in df.columns:
            df.drop(columns=['_id'], inplace=True)

    data = pd.merge(ratings, users[['user_id', 'gender']], on='user_id')
    data = pd.merge(data, movies[['item_id', 'title']], on='item_id')

    return data

def main():
    st.title("Filmat më të preferuar nga meshkujt dhe femrat")

    data = load_data()

    min_ratings = st.slider("Numri minimal i vlerësimeve për filmat:", 10, 100, 20)

    agg = data.groupby(['gender', 'title']).agg(
        avg_rating=('rating', 'mean'),
        num_ratings=('rating', 'count')
    ).reset_index()

    filtered = agg[agg['num_ratings'] >= min_ratings]

    top_male = filtered[filtered['gender'] == 'M'].sort_values('avg_rating', ascending=False).head(5)
    top_female = filtered[filtered['gender'] == 'F'].sort_values('avg_rating', ascending=False).head(5)

    st.subheader("Top 5 filmat më të preferuar nga meshkujt")
    chart_m = alt.Chart(top_male).mark_bar().encode(
        x=alt.X('avg_rating', title='Mesatarja e vlerësimeve'),
        y=alt.Y('title', sort='-x', title='Filmat'),
        tooltip=['title', 'avg_rating', 'num_ratings']
    )
    st.altair_chart(chart_m, use_container_width=True)

    st.subheader("Top 5 filmat më të preferuar nga femrat")
    chart_f = alt.Chart(top_female).mark_bar().encode(
        x=alt.X('avg_rating', title='Mesatarja e vlerësimeve'),
        y=alt.Y('title', sort='-x', title='Filmat'),
        tooltip=['title', 'avg_rating', 'num_ratings']
    )
    st.altair_chart(chart_f, use_container_width=True)

if __name__ == "__main__":
    main()
