# pylint: disable=missing-module-docstring, missing-function-docstring, missing-class-docstring, line-too-long, invalid-name
import time
import traceback
from datetime import datetime, timedelta

from media import media
from pymongo import ASCENDING, DESCENDING

from app import format_number, process_post, start_metrics_server
from mongo import posts_collection

process_start = int(time.time())
print(timedelta(seconds=0), 'Начинаем обработку.')

start_metrics_server()


def get_timedelta():
    return timedelta(seconds=time.time() - process_start)


errors = 0

threshold = int(time.time()) - (60 * 60 * 24) * 1
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

post_ids_range_set = set(range(post_id_threshold, int(
    recent_post_id + abs(post_id_threshold - recent_post_id))))
post_ids_recently_active = set(
    map(
        lambda post: post['id'],
        posts_collection.find(
            {
                'obsolete': False,
                'latest_activity': {'$gt': threshold}
            })))

post_ids = list(post_ids_range_set | post_ids_recently_active)

post_ids.sort()

posts_to_process_count = len(
    list(filter(lambda post_id: post_id < recent_post_id, post_ids)))
print(f'{get_timedelta()} Обработаем {format_number(posts_to_process_count)} постов.')

iteration_start = int(time.time())

for post_id in post_ids:
    try:
        processed_posts += 1

        if (processed_posts - posts_to_process_count) % 100 == 0:
            remaining_posts = format_number(
                processed_posts - posts_to_process_count)
            processed_percent = round(
                processed_posts / posts_to_process_count * 100)
            elapsed = max(int(time.time()) - iteration_start, 1)
            print(f"{get_timedelta()} 🐤 { remaining_posts } { processed_percent }% { str(elapsed).rjust(4, ' ') }sec { str(round((processed_posts - iteration_start_processed_posts) / elapsed)).rjust(4, ' ') }posts/sec")
            iteration_start = int(time.time())
            iteration_start_processed_posts = processed_posts

        (post, comments) = process_post(post_id)

        errors = 0
    except Exception as e:  # pylint: disable=broad-except
        traceback_str = traceback.format_exc()
        if processed_posts > posts_to_process_count:
            processed_posts -= 1
        if str(e) != '404':
            print(f"{get_timedelta()} 🐤 ❌ { format_number(processed_posts - posts_to_process_count) } { round(processed_posts / posts_to_process_count * 100) }% https://d3.ru/{post_id}/\nОшибка: {e}\n")
            print(traceback_str)
        errors += 1
        continue

print(timedelta(seconds=time.time() - process_start), 'Обработка завершена')

media()

print(timedelta(seconds=time.time() - process_start),
      'Обработка картинок завершена')
