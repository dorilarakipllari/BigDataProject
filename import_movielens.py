import requests
import zipfile
import io
import pandas as pd
from pymongo import MongoClient

# URL i datasetit
url = "https://files.grouplens.org/datasets/movielens/ml-100k.zip"
print("Po shkarkoj datasetin...")
r = requests.get(url)
r.raise_for_status()

# Hap zip-in në memory
with zipfile.ZipFile(io.BytesIO(r.content)) as z:
    # Lexo u.data
    with z.open("ml-100k/u.data") as f:
        ratings_cols = ["user_id", "item_id", "rating", "timestamp"]
        ratings_df = pd.read_csv(f, sep="\t", names=ratings_cols)

    # Lexo u.user
    with z.open("ml-100k/u.user") as f:
        users_cols = ["user_id", "age", "gender", "occupation", "zip_code"]
        users_df = pd.read_csv(f, sep="|", names=users_cols)

    # Lexo u.item
    with z.open("ml-100k/u.item") as f:
        movies_cols = ["item_id", "title", "release_date", "video_release_date", "IMDb_URL",
                       "unknown", "Action", "Adventure", "Animation", "Children's", "Comedy",
                       "Crime", "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror",
                       "Musical", "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western"]
        movies_df = pd.read_csv(f, sep="|", names=movies_cols, encoding="latin-1")

print(f"Numri i rreshtave në ratings: {len(ratings_df)}")
print(f"Numri i rreshtave në users: {len(users_df)}")
print(f"Numri i rreshtave në movies: {len(movies_df)}")

# Lidhja me MongoDB Atlas
mongo_uri = "mongodb+srv://drakipllari:admin@bigdatacluster.blg0r1y.mongodb.net/"
client = MongoClient(mongo_uri)
db = client["movielens"]

# Funksion për import të dhënash në MongoDB
def import_collection(df, collection_name):
    collection = db[collection_name]
    records = df.to_dict('records')
    if records:
        collection.insert_many(records)
        print(f"U insertuan {len(records)} dokumenta në koleksionin '{collection_name}'.")
    else:
        print(f"Nuk ka të dhëna për të importuar në koleksionin '{collection_name}'.")

# Importoj çdo DataFrame në koleksionin e tij
import_collection(ratings_df, "ratings")
import_collection(users_df, "users")
import_collection(movies_df, "movies")
