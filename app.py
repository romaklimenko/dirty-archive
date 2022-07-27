import copy
import hashlib
import json
import os
import re
import requests
import time

from mongo import posts_collection, comments_collection, media_collection

from dotenv import load_dotenv
load_dotenv()

def latest_post_activity(post):
  if 'changed' in post and post['changed'] != None:
    return max(post['created'], post['changed'])
  else:
    return post['created']

def process_post(id):
  need_line_break = False

  response = requests.get(f"https://d3.ru/api/posts/{id}/")
  if response.status_code != 200:
    raise Exception(response.status_code)
  post = response.json()
  post["_id"] = f"{post['id']}.{post['changed']}"
  post['url'] = f"https://d3.ru/{post['id']}"
  post['fetched'] = int(time.time())
  post['date'] = time.strftime('%Y-%m-%d', time.gmtime(post['created']))
  post['media'] = get_post_media(post)
  post['latest_activity'] = latest_post_activity(post)

  for media in post['media']:
    media_collection.update_one(
      { '_id': media },
      { '$addToSet': { 'usage': post['url'] } },
      upsert=True)

  result = posts_collection.replace_one({"_id": post['_id']}, post, upsert=True)
  if result.matched_count == 0:
    need_line_break = True
    print(f"💥 пост {post['url']} от {time.strftime('%Y.%m.%d', time.gmtime(post['created']))}")
  
  comments = requests.get(f"https://d3.ru/api/posts/{id}/comments/").json()['comments']
  
  for comment in comments:
    if 'body' not in comment:
      comment['body'] = ''
    
    post['latest_activity'] = max(post['latest_activity'], comment['created'])

    comment["_id"] = f"{comment['id']}.{hashlib.md5(comment['body'].encode()).hexdigest()}"
    comment["post_id"] = post["id"]
    comment['domain'] = post['domain']
    comment["url"] = f"https://d3.ru/{comment['post_id']}#{comment['id']}"
    comment['fetched'] = int(time.time())
    comment['date'] = time.strftime('%Y-%m-%d', time.gmtime(comment['created']))

    comment['media'] = get_comment_media(comment)
    for media in comment['media']:
      media_collection.update_one(
        { '_id': media },
        { '$addToSet': { 'usage': comment['url'] } },
        upsert=True)

    result = comments_collection.replace_one({"_id": comment["_id"]}, comment, upsert=True)
    if result.matched_count == 0:
      need_line_break = True
      if comment['deleted'] == True:
        print(f"💥 удаленный комментарий {comment['url']} от {time.strftime('%Y.%m.%d', time.gmtime(comment['created']))}")
      else:
        print(f"💥 комментарий {comment['url']} от {time.strftime('%Y.%m.%d', time.gmtime(comment['created']))}")

  if need_line_break:
    print()

  return (post, comments)

def format_number(number):
  return "{:,}".format(number).replace(',', ' ')

media_regex = re.compile(r"(http(s?):)\/\/cdn.jpg.wtf([/|.|\w|\s|-])*\.*\"", re.MULTILINE)

def get_media(string):
  return [match.group()[:-1] for match in re.finditer(media_regex, string)]

def get_post_media(post):
  # dirty hack
  post_copy = copy.deepcopy(post)
  post_copy['domain'] = None
  post_copy['_links'] = None
  post_copy['user'] = None
  return list(set(get_media(json.dumps(post_copy))))

def get_comment_media(comment):
  return get_media(comment['body'])
