# pylint: disable=missing-module-docstring, missing-function-docstring, missing-class-docstring, line-too-long, invalid-nameimport, disable=invalid-name

import random
import time
from datetime import timedelta
from multiprocessing import Pool, Value

from app import get_country_codes
from mongo import country_codes_collection, posts_collection

global counter  # pylint: disable=global-statement disable=global-at-module-level
counter = None


def init(args):
    global counter  # pylint: disable=global-statement
    counter = args


def get_timedelta(process_start=0):
    return timedelta(seconds=time.time() - process_start)


def process_post(post=None, process_start=0):
    method_start = time.time()

    local_counter = None

    with counter.get_lock():
        counter.value -= 1
        local_counter = counter.value

    # if country_codes_collection.count_documents({'id': post['id']}) > 0:
    #     return

    get_country_codes(post_id=post['id'])

    print(
        f'✅ {get_timedelta(process_start=process_start)} {str(local_counter).rjust(6)} {(str(int(time.time() - method_start)) + "sec").rjust(6)} https://d3.ru/{post["id"]}/ от {time.strftime("%Y.%m.%d %H:%M", time.gmtime(post["created"]))} {post["domain"]["prefix"]} {post["user"]["login"]}')


def main():
    process_start = int(time.time())

    global counter  # pylint: disable=global-statement

    print(timedelta(seconds=0), 'Начинаем обработку.')

    processes_count = 6
    posts = list(posts_collection
                 .find(
                     {
                         'obsolete': False
                     },
                     {'id': 1, 'user': 1, 'domain': 1, 'created': 1}))

    country_codes = set(country_codes_collection.distinct('_id'))

    posts = [post for post in posts if post['id'] not in country_codes]

    counter = Value('i', len(posts))

    print(timedelta(seconds=time.time() - process_start),
          f'Всего постов: {len(posts)}')

    print(timedelta(seconds=time.time() - process_start), 'Перемешиваем посты')

    random.shuffle(posts)

    print(timedelta(seconds=time.time() - process_start), 'Посты перемешаны')

    with Pool(processes=processes_count, initializer=init, initargs=(counter,)) as pool:

        # increment last paramerer
        funcs = []
        for _, post in enumerate(posts):
            funcs.append((post, process_start))

        pool.starmap(process_post, funcs)

    print(timedelta(seconds=time.time() - process_start), 'Обработка завершена')


if __name__ == '__main__':
    main()
