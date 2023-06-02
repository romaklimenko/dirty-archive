# pylint: disable=missing-module-docstring, missing-function-docstring, missing-class-docstring, line-too-long, invalid-name
import os
import random
import time
import traceback
from datetime import timedelta

from pymongo import DESCENDING

from app import format_number, process_post
from mongo import failures_collection, posts_collection

process_start = int(time.time())
print(timedelta(seconds=0), 'Начинаем обработку')


def get_timedelta():
    return timedelta(seconds=time.time() - process_start)


errors = 0

easing_threshold = int(time.time()) - (60 * 60 * 24) * 30

recent_post = posts_collection.find({}).sort('id', DESCENDING).limit(1)[0]
recent_post_id = 1_000_000
if recent_post is not None:
    recent_post_id = recent_post['id']
print(f'{get_timedelta()} Последний пост: https://d3.ru/{recent_post_id}/.')

post_id_threshold = list(
    posts_collection
    .find({'created': {'$gt': recent_post['created'] - easing_threshold}})
    .sort('created', DESCENDING)
    .limit(1))[0]['id']
print(f'{get_timedelta()} Пороговый id поста: {post_id_threshold}.')

failures_collection.delete_many({'failed': {'$lt': easing_threshold}})
print(f'{get_timedelta()} Удалили ошибки, которые произошли более порогового времени назад.')

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


post_ids = list(range(0, int(recent_post_id * 1.5)))

posts_to_skip = set()
failures_to_skip = set()

failures_to_skip = get_failures_to_skip()
print(
    f'{get_timedelta()} Пропустили посты, при получении которых произошли ошибки: {format_number(len(failures_to_skip))} шт.')

posts_to_skip = get_posts_to_skip()
print(
    f'{get_timedelta()} Пропустили посты, которые мы недавно обработали: {format_number(len(posts_to_skip))} шт.')


def should_process(post_id):  # pylint: disable=redefined-outer-name
    if post_id in posts_to_skip:
        posts_to_skip.remove(post_id)
    elif f'post_id#{post_id}' in failures_to_skip:
        failures_to_skip.remove(f'post_id#{post_id}')
    elif post_id > post_id_threshold:
        return True
    else:
        return True

    # дадим небольшой шанс
    return random.random() < 1 / int(os.getenv('ACTIVITIES_SKIP_PROBABILITY_DENOMINATOR', '1000'))


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

        if (processed_posts - posts_to_process_count) % 1000 == 0:
            remaining_posts = format_number(
                processed_posts - posts_to_process_count)
            processed_percent = round(
                processed_posts / posts_to_process_count * 100)
            elapsed = max(int(time.time()) - iteration_start, 1)
            print(f"{get_timedelta()} 🐤 { remaining_posts } { processed_percent }% { str(elapsed).rjust(4, ' ') }sec { str(round((processed_posts - iteration_start_processed_posts) / elapsed)).rjust(4, ' ') }posts/sec")
            iteration_start = int(time.time())
            iteration_start_processed_posts = processed_posts

        (post, comments) = process_post(post_id)

        failures_collection.delete_one({'_id': f'post_id#{post_id}'})

        errors = 0
    except Exception as e:  # pylint: disable=broad-except
        traceback_str = traceback.format_exc()
        failures_collection.replace_one(
            {'_id': f'post_id#{post_id}'},
            {
                '_id': f'post_id#{post_id}',
                'error': str(e),
                'traceback': traceback_str,
                'failed': int(time.time())
            }, upsert=True)
        if processed_posts > posts_to_process_count:
            processed_posts -= 1
        if str(e) != '404':
            print(f"{get_timedelta()} 🐤 ❌ { format_number(processed_posts - posts_to_process_count) } { round(processed_posts / posts_to_process_count * 100) }% https://d3.ru/{post_id}/\nОшибка: {e}\nОставшиеся попытки: {format_number(max_errors - errors)}\n")
            print(traceback_str)
        errors += 1
        continue

print(timedelta(seconds=time.time() - process_start), 'Обработка завершена')
