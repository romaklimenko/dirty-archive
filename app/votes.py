# pylint: disable=missing-module-docstring, missing-function-docstring, missing-class-docstring, line-too-long, invalid-name
import time
from datetime import timedelta

import prometheus_client
from pymongo import ASCENDING, DESCENDING

from app import process_votes, start_metrics_server
from mongo import posts_collection

process_start = int(time.time())


def get_timedelta():
    return timedelta(seconds=time.time() - process_start)


def get_dirty_votes_earliest_processed_post_timestamp():
    return posts_collection \
        .find({'votes_fetched': {'$gt': 0}, 'obsolete': False}) \
        .sort('votes_fetched', ASCENDING) \
        .limit(1)[0]['votes_fetched']


process_start = int(time.time())

start_metrics_server()

posts_collection.update_many({'votes_fetched': {'$exists': False}}, {
                             '$set': {'votes_fetched': 0}})

dirty_votes_earliest_processed_post_timestamp = get_dirty_votes_earliest_processed_post_timestamp()

DIRTY_VOTES_EARLIEST_PROCESSED_POST_TIMESTAMP = prometheus_client.Gauge(
    'dirty_votes_earliest_processed_post_timestamp', 'The timestamp of earliest vote-processed post.')

DIRTY_VOTES_EARLIEST_PROCESSED_POST_TIMESTAMP.set(
    dirty_votes_earliest_processed_post_timestamp)

print(timedelta(seconds=0), 'Начинаем обработку.')

processed_posts_count = 0

post_id = 0  # pylint: disable=invalid-name

for post in posts_collection \
        .find({'obsolete': False}) \
        .sort(
            [
                ('votes_fetched', ASCENDING),
                ('id', ASCENDING),
                ('_id', DESCENDING)
            ]):

    if time.time() - process_start > (60 * 60):
        break

    if post['id'] == post_id:
        continue
    post_id = post['id']

    processed_posts_count += 1

    print(
        f'{get_timedelta()} {processed_posts_count} https://d3.ru/{post_id}/ от {time.strftime("%Y.%m.%d %H:%M", time.gmtime(post["created"]))} {post["domain"]["prefix"]} {post["user"]["login"]}')

    if process_votes(post=post):
        DIRTY_VOTES_EARLIEST_PROCESSED_POST_TIMESTAMP.set(
            get_dirty_votes_earliest_processed_post_timestamp())

print(timedelta(seconds=time.time() - process_start), 'Обработка завершена')
