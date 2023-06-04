import os
from pymongo import MongoClient

from dotenv import load_dotenv
load_dotenv()

db = MongoClient(os.getenv('MONGO_CONNECTION_STRING',
                 'mongodb://127.0.0.1:27017/dirty'))[os.getenv('MONGO_DB_NAME', 'dirty')]
posts_collection = db[os.getenv('POSTS_COLLECTION_NAME', 'posts')]
comments_collection = db[os.getenv('COMMENTS_COLLECTION_NAME', 'comments')]
media_collection = db[os.getenv('MEDIA_COLLECTION_NAME', 'media')]
failures_collection = db[os.getenv('FAILURES_COLLECTION_NAME', 'failures')]
votes_collection = db[os.getenv('VOTES_COLLECTION_NAME', 'votes')]
