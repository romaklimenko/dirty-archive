import os
import hashlib

from pprint import pprint
from urllib.parse import urlparse
from urllib.request import urlopen

from google.cloud import storage

from app import format_number
from exif import get_image, get_exif_gps, get_exif_ifd, get_exif_tags
from mongo import media_collection

from dotenv import load_dotenv
load_dotenv()

if 'GOOGLE_APPLICATION_CREDENTIALS' not in os.environ and os.path.exists('account-key.json'):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.abspath(
        'account-key.json')

gcs_client = storage.Client()
bucket = gcs_client.get_bucket('futurico')

query = {'hash': {'$exists': False}, 'error': {
    '$ne': 'HTTP Error 404: Not Found'}}

count = media_collection.count_documents(query)
media_to_process = count

for media in media_collection.find(query):
    url = media['_id']

    print(
        f'{format_number(media_to_process - count + 1)} of {format_number(media_to_process)} ({round((media_to_process - count) / media_to_process * 100)}%)',
        url)
    count -= 1

    try:
        response = urlopen(url, timeout=60)
        content = response.read()
    except Exception as e:
        print('❌', url, e)
        media_collection.update_one({'_id': url}, {'$set': {'error': str(e)}})
        continue

    if 'error' in media:
        del media['error']
    media['content_type'] = response.info().get_content_type()
    media['hash'] = hashlib.sha256(content).hexdigest()
    media['length'] = len(content)

    media['filename'] = os.path.basename(urlparse(url).path)

    if media['content_type'] not in ['video/mp4']:

        try:
            image = get_image(content)

            media['size'] = {
                'width': image.size[0],
                'height': image.size[1]
            }

            exif = image.getexif()
            media['exif'] = {
                'tags': get_exif_tags(exif),
                'gps': get_exif_gps(exif),
                'ifd': get_exif_ifd(exif)
            }

        except Exception as e:
            print('❌ Can\'t parse image', url, e)

    try:
        blob = storage.Blob(media['hash'], bucket)
        blob.metadata = {
            'url': media['_id'],
            'filename': media['filename']
        }
        blob.upload_from_string(content, content_type=media['content_type'])
    except Exception as e:
        print('❌ Can\'t upload blob', url, e)

    try:
        pprint(media)
        media_collection.replace_one({'_id': media['_id']}, media)
    except Exception as e:
        print('❌ Can\'t update database', url, e)

    print()
