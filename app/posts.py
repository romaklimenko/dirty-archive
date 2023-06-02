# pylint: disable=missing-module-docstring, missing-function-docstring, missing-class-docstring
from mongo import posts_collection, failures_collection
from app import process_post, format_number
import random
import sys
import time
import traceback

from pymongo import ASCENDING, DESCENDING

from dotenv import load_dotenv
load_dotenv()


max_errors = 10_000
errors = 0

from_id = 0
to_id = 5 * 1_000_000

skip_if_failed_before = False

easing_threshold = int(time.time()) - (60 * 60 * 24) * 30

# один аргумент - на сколько дней назад обновлять посты и комментарии: posts.py <days_ago>
if len(sys.argv) == 2:
    days_to_refresh = int(sys.argv[1])
    recent_post = posts_collection.find({}).sort('id', DESCENDING).limit(1)[0]

    recent_post_id = recent_post['id']
    recent_post_created = recent_post['created']

    from_id = list(
        posts_collection
        .find({'created': {'$gt': recent_post['created'] - days_to_refresh * 24 * 60 * 60}})
        .sort('created', ASCENDING)
        .limit(1))[0]['id']

    posts_to_process = recent_post_id - from_id
    max_errors = min(max_errors, int(posts_to_process / 10))

    print(f"Обновляем посты с {from_id}")
else:  # без аргументов - обновляем все посты и комментарии: posts.py
    to_id = posts_collection.find({}).sort('id', DESCENDING).limit(1)[0]['id']
    posts_to_process = to_id - from_id
    skip_if_failed_before = True
    failures_collection.delete_many({'failed': {'$lt': easing_threshold}})

processed_posts = 0

iteration_start_processed_posts = processed_posts


def get_lock_timestamp():
    return int(time.time()) + (60 * 60 * 24) * 1


def get_posts_to_skip():
    condition = {
        '$and': [
            {
                'latest_activity': {'$lt': easing_threshold},
                'fetched': {'$gt': easing_threshold}
            },
            {
                '$or': [{'lock': {'$gt': time.time()}}, {'lock': {'$exists': False}}]
            }
        ]
    }
    for post in posts_collection.find(condition, {'id': 1}):
        posts_to_skip.add(post['id'])

    posts_collection.update_many(
        condition, {'$set': {'lock': get_lock_timestamp()}})

    return posts_to_skip


def get_failures_to_skip():
    condition = {
        '$and': [
            {
                'failed': {'$gt': easing_threshold},
            },
            {
                '$or': [{'lock': {'$gt': time.time()}}, {'lock': {'$exists': False}}]
            }
        ]
    }

    for failure in failures_collection.find(condition):
        failures_to_skip.add(failure['_id'])

    failures_collection.update_many(
        condition, {'$set': {'lock': get_lock_timestamp()}})

    return failures_to_skip


ids = list(range(from_id, to_id))

posts_to_skip = set()
failures_to_skip = set()

if skip_if_failed_before:
    failures_to_skip = get_failures_to_skip()
    print(
        f'Пропускаем посты, при получении которых произошли ошибки: {format_number(len(failures_to_skip))} шт.')

    posts_to_skip = get_posts_to_skip()
    print(
        f'Пропускаем посты, которые мы недавно обработали: {format_number(len(posts_to_skip))} шт.')

    def should_process(id):
        PROBABILITY = 1 / 1000  # дадим небольшой шанс

        if id in posts_to_skip:
            posts_to_skip.remove(id)
        elif f'post_id#{id}' in failures_to_skip:
            failures_to_skip.remove(f'post_id#{id}')
        else:
            return True

        return random.random() < PROBABILITY

    ids = list(filter(should_process, ids))
    posts_to_process = len(ids)

ids.sort()

iteration_start = int(time.time())

for id in ids:
    if errors > max_errors:
        break

    try:
        processed_posts += 1

        if (processed_posts - posts_to_process) % 1000 == 0:
            remaining_posts = format_number(processed_posts - posts_to_process)
            processed_percent = round(processed_posts / posts_to_process * 100)
            elapsed = max(int(time.time()) - iteration_start, 1)
            posts_in_iteration = processed_posts - iteration_start_processed_posts
            print(f"🐤 { remaining_posts } { processed_percent }% { str(elapsed).rjust(4, ' ') }sec { str(round(posts_in_iteration / elapsed)).rjust(4, ' ') }posts/sec")
            iteration_start = int(time.time())
            iteration_start_processed_posts = processed_posts

        (post, comments) = process_post(id)

        failures_collection.delete_one({'_id': f'post_id#{id}'})

        errors = 0
    except Exception as e:
        traceback_str = traceback.format_exc()
        failures_collection.replace_one(
            {'_id': f'post_id#{id}'},
            {
                '_id': f'post_id#{id}',
                'error': str(e),
                'traceback': traceback_str,
                'failed': int(time.time())
            }, upsert=True)
        if processed_posts > posts_to_process:
            processed_posts -= 1
        if str(e) != '404':
            print(f"🐤 ❌ { format_number(processed_posts - posts_to_process) } { round(processed_posts / posts_to_process * 100) }% https://d3.ru/{id}/\nОшибка: {e}\nОставшиеся попытки: {format_number(max_errors - errors)}\n")
            print(traceback_str)
        errors += 1
        continue
