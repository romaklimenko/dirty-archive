# pylint: disable=missing-module-docstring, missing-function-docstring, missing-class-docstring, line-too-long, invalid-name
import os
import random
import time
import traceback
from datetime import datetime, timedelta

import prometheus_client
from media import media
from pymongo import ASCENDING, DESCENDING

from app import format_number, process_post, start_metrics_server
from mongo import failures_collection, posts_collection

process_start = int(time.time())
print(timedelta(seconds=0), 'Начинаем обработку.')

start_metrics_server()


def get_dirty_posts_earliest_processed_post_timestamp():
    return posts_collection \
        .find({'obsolete': False, 'failed': 0}) \
        .sort('fetched', ASCENDING) \
        .limit(1)[0]['fetched']


dirty_posts_earliest_processed_post_timestamp = get_dirty_posts_earliest_processed_post_timestamp()
DIRTY_POSTS_EARLIEST_PROCESSED_POST_TIMESTAMP = prometheus_client.Gauge(
    'dirty_posts_earliest_processed_post_timestamp',
    'The timestamp of earliest processed post.')
DIRTY_POSTS_EARLIEST_PROCESSED_POST_TIMESTAMP.set(
    dirty_posts_earliest_processed_post_timestamp)


def get_timedelta():
    return timedelta(seconds=time.time() - process_start)


errors = 0

threshold = int(time.time()) - (60 * 60 * 24) * 30
print(f'{get_timedelta()} Пороговое время: {datetime.fromtimestamp(threshold)}.')

recent_post = posts_collection.find({}).sort('id', DESCENDING).limit(1)[0]
recent_post_id = None
if recent_post is not None:
    recent_post_id = recent_post['id']
    print(f'{get_timedelta()} Последний пост в базе данных: https://d3.ru/{recent_post_id}/.')
else:
    recent_post_id = 1_000_000
    print(f'{get_timedelta()} Последний пост в базе данных не найден.')

post_id_threshold = list(
    posts_collection
    .find({'created': {'$gt': threshold}})
    .sort('created', ASCENDING)
    .limit(1))[0]['id']
print(f'{get_timedelta()} Пороговый пост: https://d3.ru/{post_id_threshold}/.')

processed_posts = 0

iteration_start_processed_posts = processed_posts

post_ids = list(range(0, int(recent_post_id + 1_000)))

posts_to_skip = set(
    map(
        lambda post_id: post_id['id'],
        posts_collection.find(
            {
                'latest_activity': {'$lt': threshold},
                'fetched': {'$gt': threshold},
                'obsolete': False,
            },
            {'_id': 0, 'id': 1})))

print(
    f'{get_timedelta()} Пропустили посты, которые мы недавно обработали: {format_number(len(posts_to_skip))} шт.')

failures_collection.delete_many({'failed': {'$lt': threshold}})
failures_to_skip = set(
    map(
        lambda failure: failure['_id'],
        failures_collection.find({})))
print(
    f'{get_timedelta()} Пропустили посты, при получении которых произошли ошибки: {format_number(len(failures_to_skip))} шт.')


def should_process(post_id):  # pylint: disable=redefined-outer-name
    if post_id in posts_to_skip:
        posts_to_skip.remove(post_id)
    elif f'post_id#{post_id}' in failures_to_skip:
        failures_to_skip.remove(f'post_id#{post_id}')
    else:
        return True

    if post_id > post_id_threshold:
        return True

    # дадим небольшой шанс
    return random.random() < (1 / int(os.getenv('ACTIVITIES_SKIP_PROBABILITY_DENOMINATOR', default='1000')))


post_ids = list(filter(should_process, post_ids))
posts_to_process_count = len(
    list(filter(lambda post_id: post_id < recent_post_id, post_ids)))
print(f'{get_timedelta()} Обработаем {format_number(posts_to_process_count)} постов.')
max_errors = min(10_000, int(posts_to_process_count / 10))
print(f'{get_timedelta()} Максимальное количество ошибок: {format_number(max_errors)}.')

iteration_start = int(time.time())

for post_id in post_ids:
    if errors > max_errors:
        break

    try:
        processed_posts += 1

        if (processed_posts - posts_to_process_count) % 1_000 == 0:
            remaining_posts = format_number(
                processed_posts - posts_to_process_count)
            processed_percent = round(
                processed_posts / posts_to_process_count * 100)
            elapsed = max(int(time.time()) - iteration_start, 1)
            print(f"{get_timedelta()} 🐤 { remaining_posts } { processed_percent }% { str(elapsed).rjust(4, ' ') }sec { str(round((processed_posts - iteration_start_processed_posts) / elapsed)).rjust(4, ' ') }posts/sec")
            iteration_start = int(time.time())
            iteration_start_processed_posts = processed_posts

        (post, comments) = process_post(post_id)

        DIRTY_POSTS_EARLIEST_PROCESSED_POST_TIMESTAMP.set(
            get_dirty_posts_earliest_processed_post_timestamp())

        errors = 0
    except Exception as e:  # pylint: disable=broad-except
        traceback_str = traceback.format_exc()
        if processed_posts > posts_to_process_count:
            processed_posts -= 1
        if str(e) != '404':
            print(f"{get_timedelta()} 🐤 ❌ { format_number(processed_posts - posts_to_process_count) } { round(processed_posts / posts_to_process_count * 100) }% https://d3.ru/{post_id}/\nОшибка: {e}\nОставшиеся попытки: {format_number(max_errors - errors)}\n")
            print(traceback_str)
        errors += 1
        continue

print(timedelta(seconds=time.time() - process_start), 'Обработка завершена')

media()

print(timedelta(seconds=time.time() - process_start),
      'Обработка картинок завершена')
