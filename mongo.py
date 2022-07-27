import os
from pymongo import MongoClient

from dotenv import load_dotenv
load_dotenv()

db = MongoClient(os.environ['MONGO_CONNECTION_STRING'])[os.environ['MONGO_DB_NAME']]
posts_collection = db[os.environ['POSTS_COLLECTION_NAME']]
comments_collection = db[os.environ['COMMENTS_COLLECTION_NAME']]
media_collection = db[os.environ['MEDIA_COLLECTION_NAME']]
failures_collection = db[os.environ['FAILURES_COLLECTION_NAME']]
