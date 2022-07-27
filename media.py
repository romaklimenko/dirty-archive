import os
import hashlib

from urllib.parse import urlparse
from urllib.request import urlopen

from google.cloud import storage

from app import format_number
from exif import get_image, get_exif_gps, get_exif_ifd, get_exif_tags
from mongo import media_collection

from dotenv import load_dotenv
load_dotenv()

gcs_client = storage.Client()
bucket = gcs_client.get_bucket('futurico')

count = media_collection.count_documents({'hash': { '$exists': False } })
media_to_process = count

for media in media_collection.find({'hash': { '$exists': False } }):  
  url = media['_id']

  print(format_number(count), f'{round((media_to_process - count) / media_to_process * 100)}%', url)

  try:
    response = urlopen(url)
  except Exception as e:
    print('❌', url, e)
    continue
    
  media['content_type'] = response.info().get_content_type()
  
  content = urlopen(url).read()
  media['hash'] = hashlib.sha256(content).hexdigest()
  media['length'] = len(content)
  
  media['filename'] = os.path.basename(urlparse(url).path)

  
  if media['content_type'] not in [ 'video/mp4' ]:

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

  blob = storage.Blob(media['hash'], bucket)
  blob.metadata = {
    'url': media['_id'],
    'filename': media['filename']
  }
  blob.upload_from_string(content, content_type=media['content_type'])

  print(media)
  media_collection.replace_one({ '_id': media['_id'] }, media)

  count -= 1

  print()
