# pylint: disable=missing-module-docstring, missing-function-docstring, missing-class-docstring, line-too-long, invalid-name
import copy
import hashlib
import json
import os
import random
import re
import time
import traceback

import prometheus_client
import requests

from mongo import (comments_collection, failures_collection, media_collection,
                   posts_collection, votes_collection)

# Disable default metrics
prometheus_client.REGISTRY.unregister(prometheus_client.GC_COLLECTOR)
prometheus_client.REGISTRY.unregister(prometheus_client.PLATFORM_COLLECTOR)
prometheus_client.REGISTRY.unregister(prometheus_client.PROCESS_COLLECTOR)

# Votes
DIRTY_NEW_POST_VOTE_RECORDS_TOTAL = prometheus_client.Counter(
    'dirty_new_post_vote_records_total', 'The total number of new post vote records added.')
DIRTY_UNCHANGED_POST_VOTE_RECORDS_TOTAL = prometheus_client.Counter(
    'dirty_unchanged_post_vote_records_total', 'The total number of unchanged post vote records.')

# Comments
DIRTY_NEW_COMMENT_VOTE_RECORDS_TOTAL = prometheus_client.Counter(
    'dirty_new_comment_vote_records_total', 'The total number of new comment vote records added.')
DIRTY_UNCHANGED_COMMENT_VOTE_RECORDS_TOTAL = prometheus_client.Counter(
    'dirty_unchanged_comment_vote_records_total', 'The total number of unchanged comment vote records.')
DIRTY_NEW_COMMENT_RECORDS_TOTAL = prometheus_client.Counter(
    'dirty_new_comment_records_total', 'The total number of new comment records added.')
DIRTY_UNCHANGED_COMMENT_RECORDS_TOTAL = prometheus_client.Counter(
    'dirty_unchanged_comment_records_total', 'The total number of unchanged comment records.')

# Posts
DIRTY_VOTES_PROCESSED_POSTS_TOTAL = prometheus_client.Counter(
    'dirty_votes_processed_posts_total', 'The total number of posts for which votes were processed.')
DIRTY_VOTES_PROCESSED_POSTS_ERRORS_TOTAL = prometheus_client.Counter(
    'dirty_votes_processed_posts_errors_total', 'The total number of errors during votes processing.')

DIRTY_POSTS_PROCESSED_POSTS_TOTAL = prometheus_client.Counter(
    'dirty_posts_processed_posts_total', 'The total number of posts processed.')
DIRTY_POSTS_PROCESSED_POSTS_ERRORS_TOTAL = prometheus_client.Counter(
    'dirty_posts_processed_posts_errors_total', 'The total number of errors during posts processing.')

DIRTY_NEW_POST_RECORDS_TOTAL = prometheus_client.Counter(
    'dirty_new_post_records_total', 'The total number of new post records added.')
DIRTY_UNCHANGED_POST_RECORDS_TOTAL = prometheus_client.Counter(
    'dirty_unchanged_post_records_total', 'The total number of unchanged post records.')

# Media
DIRTY_NEW_MEDIA_RECORDS_TOTAL = prometheus_client.Counter(
    'dirty_new_media_records_total', 'The total number of new media records added.')
DIRTY_UNCHANGED_MEDIA_RECORDS_TOTAL = prometheus_client.Counter(
    'dirty_unchanged_media_records_total', 'The total number of unchanged media records.')


def start_metrics_server(port=8000):
    prometheus_client.start_http_server(port)


posts_to_vote_process_cache = {
    'post_ids': set(),
    'expires': 0
}

DIRTY_POSTS_PROCESS_POST_SECONDS = prometheus_client.Summary(
    'dirty_posts_process_post_seconds', 'The time it takes to process a post.')


@DIRTY_POSTS_PROCESS_POST_SECONDS.time()
def process_post(post_id):
    method_start = time.time()
    try:
        need_line_break = False

        response = requests.get(
            f"https://d3.ru/api/posts/{post_id}/", timeout=30)
        if response.status_code != 200:
            raise Exception(  # pylint: disable=broad-exception-raised
                response.status_code)
        post = response.json()

        post["_id"] = f"{post['id']}.{post['changed']}"
        post['url'] = f"https://d3.ru/{post['id']}"
        post['fetched'] = int(time.time())
        post['failed'] = 0
        post['date'] = time.strftime('%Y-%m-%d', time.gmtime(post['created']))
        post['month'] = time.strftime('%Y-%m', time.gmtime(post['created']))
        post['year'] = time.strftime('%Y', time.gmtime(post['created']))
        post['media'] = get_post_media(post)
        post['obsolete'] = False

        for media in post['media']:
            result = media_collection.update_one(
                {'_id': media},
                {'$addToSet': {'usage': post['url'], 'ts': post['created']}},
                upsert=True)
            if result.matched_count == 0:
                DIRTY_NEW_MEDIA_RECORDS_TOTAL.inc()
            else:
                DIRTY_UNCHANGED_MEDIA_RECORDS_TOTAL.inc()

        comments_response = requests.get(
            f"https://d3.ru/api/posts/{post_id}/comments/", timeout=30)

        comments = []

        if comments_response.status_code == 200:
            comments = comments_response.json()['comments']

        # latest_activity
        post['latest_activity'] = post['created']

        for comment in comments:
            post['latest_activity'] = max(
                post['latest_activity'], comment['created'])

        # refresh posts_to_vote_process_cache
        if posts_to_vote_process_cache['expires'] < time.time():
            post_ids = set(map(lambda x: x[id], posts_collection.find(
                {
                    'id': post['id'],
                    'obsolete': False,
                    'votes_fetched': {'$lt': time.time() - (60 * 60 * 24 * 30)}
                },
                {'_id': 0, 'id': 1})))
            posts_to_vote_process_cache['post_ids'] = post_ids
            posts_to_vote_process_cache['expires'] = time.time() + 60 * 60

        should_fetch_votes = \
            post['latest_activity'] > time.time() - (60 * 60 * 24 * 30) or \
            post['id'] in posts_to_vote_process_cache['post_ids'] or \
            random.random() < (
                1 / int(os.getenv('ACTIVITIES_SKIP_PROBABILITY_DENOMINATOR', default='1000')))

        post_country_code = None
        comments_country_codes = None

        if post['latest_activity'] > 1497484800:  # 2017-06-15 - дата, с которой появились флажки
            post_country_code, comments_country_codes = get_country_codes(
                post_id)

        if post_country_code is not None and post_country_code != '':
            post['country_code'] = post_country_code

        result = posts_collection.update_one(
            {"_id": post['_id']}, {'$set': post}, upsert=True)
        if result.matched_count == 0:
            DIRTY_NEW_POST_RECORDS_TOTAL.inc()
            should_fetch_votes = True
            need_line_break = True
            print(
                f"💥 пост {post['url']} от {time.strftime('%Y.%m.%d %H:%M', time.gmtime(post['created']))} {post['domain']['prefix']} {post['user']['login']}: \"{post['title']}\"")
        else:
            DIRTY_UNCHANGED_POST_RECORDS_TOTAL.inc()

        # помечаем устаревшие записи
        posts_collection.update_many(
            {'_id': {'$ne': post['_id']}, 'id': post['id']},
            {'$set': {'obsolete': True}})

        for comment in comments:
            if 'body' not in comment:
                comment['body'] = ''

            comment["_id"] = f"{comment['id']}.{hashlib.md5(comment['body'].encode()).hexdigest()}"
            comment["post_id"] = post["id"]
            comment['domain'] = post['domain']
            comment["url"] = f"https://d3.ru/{comment['post_id']}#{comment['id']}"
            comment['fetched'] = int(time.time())
            comment['date'] = time.strftime(
                '%Y-%m-%d', time.gmtime(comment['created']))
            if comments_country_codes is not None and comment['id'] in comments_country_codes and comments_country_codes[comment['id']] != '':
                comment['country_code'] = comments_country_codes[comment['id']]

            comment['media'] = get_comment_media(comment)
            for media in comment['media']:
                result = media_collection.update_one(
                    {'_id': media},
                    {'$addToSet': {
                        'usage': comment['url'], 'ts': comment['created']}},
                    upsert=True)
                if result.matched_count == 0:
                    DIRTY_NEW_MEDIA_RECORDS_TOTAL.inc()
                else:
                    DIRTY_UNCHANGED_MEDIA_RECORDS_TOTAL.inc()

            result = comments_collection.replace_one(
                {"_id": comment["_id"]}, comment, upsert=True)
            if result.matched_count == 0:
                DIRTY_NEW_COMMENT_RECORDS_TOTAL.inc()
                should_fetch_votes = True
                need_line_break = True
                if comment['deleted'] is True:
                    print(
                        f"💥 удаленный комментарий {comment['url']} от {time.strftime('%Y.%m.%d %H:%M', time.gmtime(comment['created']))} {comment['domain']['prefix']} {comment['user']['login']}")
                else:
                    print(
                        f"💥 комментарий {comment['url']} от {time.strftime('%Y.%m.%d %H:%M', time.gmtime(comment['created']))} {comment['domain']['prefix']} {comment['user']['login']}")
            else:
                DIRTY_UNCHANGED_COMMENT_RECORDS_TOTAL.inc()

        if should_fetch_votes:
            process_votes(post=post)

        if need_line_break:
            print()

        failures_collection.delete_one({'_id': f'post_id#{post_id}'})

        DIRTY_POSTS_PROCESSED_POSTS_TOTAL.inc()

        method_elapsed = time.time() - method_start

        if method_elapsed > 60:
            print(
                f"⚠️  {int(method_elapsed)} секунд на обработку поста https://d3.ru/{post_id}/")

        return (post, comments)
    except Exception as e:
        DIRTY_POSTS_PROCESSED_POSTS_ERRORS_TOTAL.inc()
        traceback_str = traceback.format_exc()
        failures_collection.replace_one(
            {'_id': f'post_id#{post_id}'},
            {
                '_id': f'post_id#{post_id}',
                'error': str(e),
                'traceback': traceback_str,
                'failed': int(time.time())
            }, upsert=True)
        posts_collection.update_many(
            {'id': post_id, 'obsolete': False}, {'$set': {'failed': time.time()}})

        method_elapsed = time.time() - method_start

        if method_elapsed > 60:
            print(
                f"⚠️  {int(method_elapsed)} секунд на обработку поста https://d3.ru/{post_id}/")

        raise e


def get_country_codes(post_id):
    try:
        post_url = f'https://d3.ru/{post_id}/'
        cookies = dict(sid=os.environ['SID'], uid=os.environ['UID'])
        text_response = requests.get(
            post_url, cookies=cookies, timeout=30).text
        string_to_find = '            window.entryStorages[window.pageName] = '
        for i, line in enumerate(str.splitlines(text_response)):
            if line.startswith(string_to_find):
                entry_storages = json.loads(
                    str
                    .splitlines(text_response)[i]
                    .replace(string_to_find, ''))
                post_country_code = entry_storages['post']['country_code']

                comments_country_codes = dict()
                for comment in entry_storages['comments']:
                    comments_country_codes[comment['id']
                                           ] = comment['country_code']

                return (post_country_code, comments_country_codes)
    except Exception as e:  # pylint: disable=broad-except
        print('get_country_codes', e)
        return (None, dict())


def format_number(number):
    return "{:,}".format(number).replace(',', '_')


media_regex = re.compile(
    r"(http(s?):)\/\/cdn.jpg.wtf([/|.|\w|\s|-])*\.*\"", re.MULTILINE)


def get_media(string):
    return [match.group()[:-1] for match in re.finditer(media_regex, string)]


def get_post_media(post):
    # dirty hack
    post_copy = copy.deepcopy(post)
    post_copy['domain'] = None
    post_copy['_links'] = None
    post_copy['user'] = None
    return list(set(get_media(json.dumps(post_copy))))


def get_comment_media(comment):
    return get_media(comment['body'])


def upsert_post_vote(post, vote):
    post_id = post['id']

    domain_prefix = ''
    if 'domain' in post and 'prefix' in post['domain']:
        domain_prefix = post['domain']['prefix']

    doc = {
        '_id': f"{post_id}|{vote['user']['login']}|{vote['user']['id']}|{vote['vote']}",
        'post_id': post_id,
        'domain': domain_prefix,
        'vote': vote['vote'],
        'changed': vote['changed'],
        'from_user_login': vote['user']['login'],
        'from_user_id': vote['user']['id'],
        'to_user_login': post['user']['login'],
        'to_user_id': post['user']['id'],
        'delta': vote['changed'] - post['created']
    }

    result = votes_collection.replace_one(
        {'_id': doc['_id']},
        doc,
        upsert=True)
    if result.matched_count == 0:
        DIRTY_NEW_POST_VOTE_RECORDS_TOTAL.inc()
    else:
        DIRTY_UNCHANGED_POST_VOTE_RECORDS_TOTAL.inc()


def upsert_comment_vote(comment, vote):
    post_id = comment['post_id']
    comment_id = comment['id']

    domain_prefix = ''
    if 'domain' in comment and 'prefix' in comment['domain']:
        domain_prefix = comment['domain']['prefix']

    doc = {
        '_id': f"{post_id}|{comment_id}|{vote['user']['login']}|{vote['user']['id']}|{vote['vote']}",
        'post_id': post_id,
        'comment_id': comment_id,
        'domain': domain_prefix,
        'vote': vote['vote'],
        'from_user_login': vote['user']['login'],
        'from_user_id': vote['user']['id'],
        'to_user_login': comment['user']['login'],
        'to_user_id': comment['user']['id'],
        'delta': vote['changed'] - comment['created']
    }

    result = votes_collection.replace_one(
        {'_id': doc['_id']},
        doc,
        upsert=True)

    if result.matched_count == 0:
        DIRTY_NEW_COMMENT_VOTE_RECORDS_TOTAL.inc()
    else:
        DIRTY_UNCHANGED_COMMENT_VOTE_RECORDS_TOTAL.inc()


def process_post_votes(post):
    post_id = post['id']

    headers = {
        'X-Futuware-UID': os.environ['UID'],
        'X-Futuware-SID': os.environ['SID']
    }

    page = 1
    url = f'https://d3.ru/api/posts/{post_id}/votes/?per_page=210&page={page}'
    response = requests.get(url, headers=headers, timeout=30).json()

    if response is not None and 'status' in response and response['status'] == 'error':
        print('❌', 'process_post_votes', response)
        return

    while response is not None and response['upvotes'] is not None and response['downvotes'] is not None:
        if response['upvotes'] == [] and response['downvotes'] == []:
            break
        if response['upvotes'] is not None:
            for vote in response['upvotes']:
                upsert_post_vote(post=post, vote=vote)
        if response['downvotes'] is not None:
            for vote in response['downvotes']:
                upsert_post_vote(post=post, vote=vote)

        if response['page'] == response['page_count']:
            break

        page += 1
        url = f'https://d3.ru/api/posts/{post_id}/votes/?per_page=210&page={page}'
        response = requests.get(url, headers=headers, timeout=30).json()


def process_comment_votes(comment):
    comment_id = comment['id']

    headers = {
        'X-Futuware-UID': os.environ['UID'],
        'X-Futuware-SID': os.environ['SID']
    }

    page = 1
    url = f'https://d3.ru/api/comments/{comment_id}/votes/?per_page=210&page={page}'
    response = requests.get(url, headers=headers, timeout=30).json()

    if response is not None and 'status' in response and response['status'] == 'error':
        print('❌', 'process_comment_votes', response)
        # {'status': 'error', 'errors': [{'description': {'code': 'deleted'}, 'location': 'path', 'name': 'comment_id'}]}
        if 'errors' in response and len(response['errors']) > 0 and 'description' in response['errors'][0] and response['errors'][0]['description']['code'] == 'deleted':
            comments_collection.update_many(
                {'id': comment_id},
                {'$set': {'deleted': True}}
            )
            print(
                f"💥 удаленный комментарий {comment['url']} от {time.strftime('%Y.%m.%d %H:%M', time.gmtime(comment['created']))} {comment['domain']['prefix']} {comment['user']['login']}")
        return

    while response is not None and response['upvotes'] is not None and response['downvotes'] is not None:
        if response['upvotes'] == [] and response['downvotes'] == []:
            break
        if response['upvotes'] is not None:
            for vote in response['upvotes']:
                upsert_comment_vote(comment=comment, vote=vote)
        if response['downvotes'] is not None:
            for vote in response['downvotes']:
                upsert_comment_vote(comment=comment, vote=vote)

        if response['page'] == response['page_count']:
            break

        page += 1
        url = f'https://d3.ru/api/comments/{comment_id}/votes/?per_page=210&page={page}'
        response = requests.get(url, headers=headers, timeout=30).json()


DIRTY_VOTES_PROCESS_VOTES_SECONDS = prometheus_client.Summary(
    'dirty_votes_process_votes_seconds', 'The time it takes to process votes.')


@DIRTY_VOTES_PROCESS_VOTES_SECONDS.time()
def process_votes(post):
    try:
        process_post_votes(post=post)

        for comment in comments_collection.find({'post_id': post['id'], 'deleted': False}):
            if comment['rating'] is None:
                continue
            process_comment_votes(comment=comment)

        posts_collection.update_many(
            {'id': post['id']}, {'$set': {'votes_fetched': int(time.time())}})
        DIRTY_VOTES_PROCESSED_POSTS_TOTAL.inc()
        return True
    except Exception as e:  # pylint: disable=broad-exception-caught
        DIRTY_VOTES_PROCESSED_POSTS_ERRORS_TOTAL.inc()
        traceback_str = traceback.format_exc()
        print('❌ process_votes:', e, traceback_str)
        return False
