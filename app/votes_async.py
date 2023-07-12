# pylint: disable=missing-module-docstring, missing-function-docstring, missing-class-docstring, line-too-long, invalid-nameimport, disable=invalid-name

import time
from datetime import timedelta
from multiprocessing import Pool, Value

import random

from pymongo import ASCENDING, DESCENDING

from app import process_votes
from mongo import posts_collection

global counter  # pylint: disable=global-statement disable=global-at-module-level
counter = None


def init(args):
    global counter  # pylint: disable=global-statement
    counter = args


def get_timedelta(process_start=0):
    return timedelta(seconds=time.time() - process_start)


def process_post(post=None, process_start=0):
    local_counter = None

    with counter.get_lock():
        counter.value += 1
        local_counter = counter.value

    # if time.time() - process_start > (60 * 60):
    #     return

    print(
        f'{get_timedelta(process_start=process_start)} {local_counter} https://d3.ru/{post["id"]}/ от {time.strftime("%Y.%m.%d %H:%M", time.gmtime(post["created"]))} {post["domain"]["prefix"]} {post["user"]["login"]}')

    process_votes(post=post)

    print(
        f'{get_timedelta(process_start=process_start)} {local_counter} https://d3.ru/{post["id"]}/ обработан')


def main():
    process_start = int(time.time())

    global counter  # pylint: disable=global-statement

    counter = Value('i', 0)

    posts_collection.update_many({'votes_fetched': {'$exists': False}}, {
        '$set': {'votes_fetched': 0}})

    print(timedelta(seconds=0), 'Начинаем обработку.')

    processes_count = 5
    # posts = list(posts_collection
    #              .find(
    #                  {
    #                      {'$lt': time.time() - (60 * 60 * 24 * 30) + 60 * 60},
    #                      'votes_fetched': 0
    #                  })
    #              .sort(
    #                  [
    #                      ('votes_fetched', ASCENDING),
    #                      ('id', ASCENDING),
    #                      ('_id', DESCENDING)
    #                  ]))
    posts = list(posts_collection
                 .aggregate([
                     #  {'$match': {'votes_fetched': {'$lt': time.time() - (60 * 60 * 24 * 30) + 60 * 60}}},
                     {'$match': {'votes_fetched': 0}},
                     {'$sample': {'size': 10_000}},
                 ]))

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
