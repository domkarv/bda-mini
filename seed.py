import pandas as pd
from pymongo import MongoClient

# Load CSV file
csv_file = "ecommerce_data.csv"  # Replace with your CSV file path
df = pd.read_csv(csv_file, encoding="ISO-8859-1")  # or 'latin1'

# Convert CSV to JSON format
json_data = df.to_dict(orient="records")

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")  # Replace with your MongoDB URI
db = client["bda"]  # Replace with your database name
collection = db["ecommerce"]  # Replace with your collection name

# Insert JSON data into MongoDB
collection.insert_many(json_data)

print("Data successfully inserted into MongoDB!")
