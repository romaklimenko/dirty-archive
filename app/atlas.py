import os

from pymongo import MongoClient

from mongo import media_collection as local_media_collection

from dotenv import load_dotenv
load_dotenv()

atlas_db = MongoClient(os.environ['ATLAS'])['dirty']

count = 0

docs = list()

for doc in local_media_collection.find(
    {'content_type': {'$regex': '^(image|video)'}, 'usage.1': {
        '$exists': True}},
        {'usage': 1, 'content_type': 1}):
    count += 1
    doc['url'] = doc['_id']
    doc['_id'] = count
    docs.append(doc)

print(f'uploading {count} documents to atlas:')

atlas_db.drop_collection('media')
atlas_media_collection = atlas_db['media']

atlas_media_collection.insert_many(docs)

###

# count = 0

# docs = list()

# for doc in local_media_collection.find(
#     {'selected': True},
#         {'usage': 1, 'content_type': 1}):
#     count += 1
#     doc['url'] = doc['_id']
#     doc['_id'] = count
#     docs.append(doc)

# print(count)

# atlas_db.drop_collection('media_selected')
# atlas_media_selected_collection = atlas_db['media_selected']

# atlas_media_selected_collection.insert_many(docs)
