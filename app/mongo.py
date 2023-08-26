"""MongoDB connection details."""

import os

from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

db = MongoClient(os.getenv('MONGO_CONNECTION_STRING',
                 'mongodb://127.0.0.1:27017/dirty'))[os.getenv('MONGO_DB_NAME', 'dirty')]
posts_collection = db['posts']
comments_collection = db['comments']
country_codes_collection = db['country_codes']
media_collection = db['media']
failures_collection = db['failures']
votes_collection = db['votes']
