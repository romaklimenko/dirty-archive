import random
import sys
import time
import traceback

from pymongo import ASCENDING, DESCENDING

from app import process_post, format_number
from mongo import posts_collection, failures_collection

from dotenv import load_dotenv
load_dotenv()

max_errors = 10_000
errors = 0

from_id = 0
to_id = 5 * 1_000_000

skip_if_failed_before = False

easing_threshold = int(time.time()) - (60 * 60 * 24) * 30

if len(sys.argv) == 3:  # два аргумента - начальный и конечный id поста: posts.py <from_id> <to_id>
    from_id = int(sys.argv[1])
    to_id = int(sys.argv[2])
    posts_to_process = to_id - from_id
elif len(sys.argv) == 2:  # один аргумент - на сколько дней назад обновлять посты и комментарии: posts.py <days_ago>
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

iteration_start = int(time.time())
iteration_start_processed_posts = processed_posts

ids = list(range(from_id, to_id))
if len(sys.argv) < 2:
    random.shuffle(ids)

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

        force_fetch = random.random() < 1 / 1000

        if not force_fetch:

            # если мы уже спотыкались на этом посте ранее
            if skip_if_failed_before and failures_collection.count_documents({'_id': f'post_id#{id}'}) > 0:
                continue  # пропускаем его, но не сбрасываем счетчик ошибок

            if (posts_collection.count_documents(
                    {
                        'id': id,
                        'fetched': {'$gt': easing_threshold},
                        'latest_activity': {'$lt': easing_threshold}
                    }) > 0):
                errors = 0
                continue

        (post, comments) = process_post(id)

        if not skip_if_failed_before or force_fetch:
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
