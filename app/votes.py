# pylint: disable=missing-module-docstring, missing-function-docstring, missing-class-docstring, line-too-long, invalid-nameimport, disable=invalid-name
import time
from datetime import timedelta

import prometheus_client

from pymongo import ASCENDING, DESCENDING

from app import process_votes, start_metrics_server
from mongo import posts_collection

process_start = int(time.time())


def get_timedelta():
    return timedelta(seconds=time.time() - process_start)


process_start = int(time.time())

start_metrics_server()

posts_collection.update_many({'votes_fetched': {'$exists': False}}, {
                             '$set': {'votes_fetched': 0}})

dirty_votes_processed_more_than_30_days_ago_posts_total = posts_collection.count_documents(
    {'votes_fetched': {'$lt': int(time.time()) - (60 * 60 * 24) * 30}, 'obsolete': False})

DIRTY_VOTES_PROCESSED_MORE_THAN_30_DAYS_AGO_POSTS_TOTAL = prometheus_client.Gauge(
    'dirty_votes_processed_more_than_30_days_ago_posts_total', 'The total number of posts votes-processed more than 30 days ago.')

DIRTY_VOTES_PROCESSED_MORE_THAN_30_DAYS_AGO_POSTS_TOTAL.set(
    dirty_votes_processed_more_than_30_days_ago_posts_total)

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
        dirty_votes_processed_more_than_30_days_ago_posts_total -= 1
        DIRTY_VOTES_PROCESSED_MORE_THAN_30_DAYS_AGO_POSTS_TOTAL.set(
            dirty_votes_processed_more_than_30_days_ago_posts_total)
    if processed_posts_count % 100 == 0:
        dirty_votes_processed_more_than_30_days_ago_posts_total = posts_collection.count_documents(
            {'votes_fetched': {'$lt': int(time.time()) - (60 * 60 * 24) * 30}, 'obsolete': False})

print(timedelta(seconds=time.time() - process_start), 'Обработка завершена')
