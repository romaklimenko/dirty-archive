# pylint: disable=missing-module-docstring, missing-function-docstring, missing-class-docstring, line-too-long, invalid-nameimport, disable=invalid-name
import time
from datetime import timedelta

from pymongo import ASCENDING, DESCENDING

from app import process_post_votes, process_comment_votes, format_number
from mongo import db, posts_collection, comments_collection, votes_collection


process_start = int(time.time())


def get_timedelta():
    return timedelta(seconds=time.time() - process_start)


process_start = int(time.time())

posts_collection.update_many({'votes_fetched': {'$exists': False}}, {
                             '$set': {'votes_fetched': 0}})

print(timedelta(seconds=0), 'Начинаем обработку.')

posts = list(posts_collection
             .find({'votes_fetched': {'$lt': time.time() - (60 * 60 * 24 * 30)}})
             .sort([('votes_fetched', ASCENDING), ('id', ASCENDING), ('_id', DESCENDING)]))

posts_count = len(posts)
processed_posts_count = 0

post_id = 0  # pylint: disable=invalid-name

for post in posts:

    if time.time() - process_start > (60 * 60):
        break

    if post['id'] == post_id:
        continue
    post_id = post['id']

    processed_posts_count += 1

    print(
        f'{get_timedelta()} {format_number(processed_posts_count)} ({format_number(posts_count - processed_posts_count)}) https://d3.ru/{post_id}/ от {time.strftime("%Y.%m.%d %H:%M", time.gmtime(post["created"]))} {post["domain"]["prefix"]} {post["user"]["login"]}')

    process_post_votes(post=post)

    comment_count = 0  # pylint: disable=invalid-name
    deleted_comment_ids = set(comment['id'] for comment in comments_collection.find(
        {'post_id': post['id'], 'deleted': True}))
    for comment in comments_collection.find({'post_id': post['id'], 'deleted': False}):
        if time.time() - process_start > (60 * 60):
            break

        comment_count += 1

        if comment['id'] in deleted_comment_ids:
            continue
        process_comment_votes(comment=comment)
    print(f'{get_timedelta()}\tКомментарии: {comment_count}')
    if len(deleted_comment_ids) > 0:
        print(f'{get_timedelta()}\tУдаленные комментарии: {len(deleted_comment_ids)}')

    posts_collection.update_many(
        {'id': post['id']}, {'$set': {'votes_fetched': int(time.time())}})

print(timedelta(seconds=time.time() - process_start), 'Обработка завершена')

# print stats
votes_count = votes_collection.count_documents({})
voted_posts_count = len(votes_collection.distinct('post_id'))
print(timedelta(seconds=time.time() - process_start),
      f'Обработано {format_number(votes_count)} голосов в {format_number(voted_posts_count)} постах. В среднем {votes_count / voted_posts_count} голосов на пост.')  # pylint: disable=line-too-long

un_processed_posts_count = posts_collection.count_documents(
    {'votes_fetched': {'$lt': time.time() - 60 * 60 * 24 * 30}})
posts_count = posts_collection.count_documents({})
print(timedelta(seconds=time.time() - process_start),
      f'Обработано {format_number(posts_count - un_processed_posts_count)} постов из {format_number(posts_count)} ({(posts_count - un_processed_posts_count) / posts_count}).')

total_votes_size = int(db.command('collstats', 'votes')[
                       'totalSize'] / 1024 / 1024 / 1024 * 100) / 100

print(timedelta(seconds=time.time() - process_start),
      f'votes: {total_votes_size} GB')
