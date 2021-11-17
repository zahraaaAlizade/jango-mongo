import json
from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client['city']
collection_currency = db['data1']
with open('data.json',encoding="utf8") as f:
    file_data = json.load(f)
collection_currency.insert_many(file_data)
client.close()

