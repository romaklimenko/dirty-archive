# pylint: disable=missing-module-docstring, missing-function-docstring, missing-class-docstring, line-too-long, invalid-nameimport, disable=invalid-name

import json
import time
from datetime import timedelta
from multiprocessing import Pool, Value

from mongo import comments_collection, country_codes_collection

global counter  # pylint: disable=global-statement disable=global-at-module-level
counter = None


def init(args):
    global counter  # pylint: disable=global-statement
    counter = args


def get_timedelta(process_start=0):
    return timedelta(seconds=time.time() - process_start)


def process_post(data=None, process_start=0):
    method_start = time.time()

    local_counter = None

    with counter.get_lock():
        counter.value -= 1
        local_counter = counter.value

    comments_collection.update_many(
        {'id': data['id']},
        {'$set': {'country_code': data['country_code']}})

    if (local_counter) % 1_000 == 0:
        print(
            f'✅ {get_timedelta(process_start=process_start)} {str(local_counter).rjust(6)} {(str(int(time.time() - method_start)) + "sec").rjust(6)} {data["id"]} {data["country_code"]}')


def main():
    process_start = int(time.time())

    global counter  # pylint: disable=global-statement

    print(timedelta(seconds=0), 'Начинаем обработку.')

    processes_count = 10

    comments_with_country_code = set(
        map(
            lambda x: x['_id'],
            comments_collection.aggregate([
                {'$match': {'country_code': {'$ne': ''}}},
                {'$group': {'_id': '$id'}}
            ], allowDiskUse=True)))

    print(timedelta(seconds=time.time() - process_start),
          'Комментарии с кодом страны:', len(comments_with_country_code))

    comments_country_codes = {}
    for i, doc in enumerate(list(country_codes_collection.find())):
        if i % 100_000 == 0:
            print(timedelta(seconds=time.time() - process_start),
                  'Обработано стран:', i)
        comment_country_codes_str_keys = json.loads(
            doc['comments_country_codes'])
        for k, v in comment_country_codes_str_keys.items():
            if v != '' and int(k) not in comments_with_country_code:
                comments_country_codes[int(k)] = v

    print(timedelta(seconds=time.time() - process_start),
          'Комментарии с кодом страны в кэше:', len(comments_country_codes.keys()))

    counter = Value('i', len(comments_country_codes.keys()))

    with Pool(processes=processes_count, initializer=init, initargs=(counter,)) as pool:

        # increment last paramerer
        funcs = []
        for k, v in comments_country_codes.items():
            funcs.append(({'id': k, 'country_code': v}, process_start))

        pool.starmap(process_post, funcs)

    print(timedelta(seconds=time.time() - process_start), 'Обработка завершена')


if __name__ == '__main__':
    main()
