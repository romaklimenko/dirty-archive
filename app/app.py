# pylint: disable=missing-module-docstring, missing-function-docstring, missing-class-docstring, line-too-long, invalid-name
import os
import copy
import hashlib
import json
import re
import time
import traceback

import requests

from mongo import comments_collection, media_collection, posts_collection, votes_collection


def latest_post_activity(post):
    if 'changed' in post and post['changed'] is not None:
        return max(post['created'], post['changed'])
    return post['created']


def process_post(post_id):
    need_line_break = False

    response = requests.get(f"https://d3.ru/api/posts/{post_id}/", timeout=30)
    if response.status_code != 200:
        raise Exception(  # pylint: disable=broad-exception-raised
            response.status_code)
    post = response.json()
    post["_id"] = f"{post['id']}.{post['changed']}"
    post['url'] = f"https://d3.ru/{post['id']}"
    post['fetched'] = int(time.time())
    post['date'] = time.strftime('%Y-%m-%d', time.gmtime(post['created']))
    post['media'] = get_post_media(post)
    post['latest_activity'] = latest_post_activity(post)

    for media in post['media']:
        media_collection.update_one(
            {'_id': media},
            {'$addToSet': {'usage': post['url'], 'ts': post['created']}},
            upsert=True)

    result = posts_collection.replace_one(
        {"_id": post['_id']}, post, upsert=True)
    if result.matched_count == 0:
        need_line_break = True
        print(
            f"💥 пост {post['url']} от {time.strftime('%Y.%m.%d', time.gmtime(post['created']))}")

    if post['created'] > time.time() - (60 * 60 * 24 * 7):
        process_post_votes(post_id, post['created'])

    comments_response = requests.get(
        f"https://d3.ru/api/posts/{post_id}/comments/", timeout=30)

    comments = []

    if comments_response.status_code == 200:
        comments = comments_response.json()['comments']

    for comment in comments:
        if 'body' not in comment:
            comment['body'] = ''

        post['latest_activity'] = max(
            post['latest_activity'], comment['created'])

        comment["_id"] = f"{comment['id']}.{hashlib.md5(comment['body'].encode()).hexdigest()}"
        comment["post_id"] = post["id"]
        comment['domain'] = post['domain']
        comment["url"] = f"https://d3.ru/{comment['post_id']}#{comment['id']}"
        comment['fetched'] = int(time.time())
        comment['date'] = time.strftime(
            '%Y-%m-%d', time.gmtime(comment['created']))

        comment['media'] = get_comment_media(comment)
        for media in comment['media']:
            media_collection.update_one(
                {'_id': media},
                {'$addToSet': {
                    'usage': comment['url'], 'ts': comment['created']}},
                upsert=True)

        result = comments_collection.replace_one(
            {"_id": comment["_id"]}, comment, upsert=True)
        if result.matched_count == 0:
            need_line_break = True
            if comment['deleted'] is True:
                print(
                    f"💥 удаленный комментарий {comment['url']} от {time.strftime('%Y.%m.%d', time.gmtime(comment['created']))}")
            else:
                print(
                    f"💥 комментарий {comment['url']} от {time.strftime('%Y.%m.%d', time.gmtime(comment['created']))}")

        if not comment['deleted'] and comment['created'] > time.time() - (60 * 60 * 24 * 7):
            process_comment_votes(post_id, comment['id'], comment['created'])

    if need_line_break:
        print()

    return (post, comments)


def format_number(number):
    return "{:,}".format(number).replace(',', ' ')


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


def upsert_post_vote(post_id, post_created, vote):
    doc = {
        'post_id': post_id,
        'vote': vote['vote'],
        'changed': vote['changed'],
        'user_login': vote['user']['login'],
        'user_id': vote['user']['id'],
        'delta': vote['changed'] - post_created
    }

    votes_collection.update_one(
        {
            'post_id': post_id,
            'vote': vote['vote'],
            'changed': vote['changed'],
            'user_login': vote['user']['login'],
            'user_id': vote['user']['id']
        },
        {'$set': doc},
        upsert=True)


def upsert_comment_vote(post_id, comment_id, comment_created, vote):
    doc = {
        'post_id': post_id,
        'comment_id': comment_id,
        'vote': vote['vote'],
        'changed': vote['changed'],
        'user_login': vote['user']['login'],
        'user_id': vote['user']['id'],
        'delta': vote['changed'] - comment_created
    }

    votes_collection.update_one(
        {
            'post_id': post_id,
            'comment_id': post_id,
            'vote': vote['vote'],
            'changed': vote['changed'],
            'user_login': vote['user']['login'],
            'user_id': vote['user']['id']
        },
        {'$set': doc},
        upsert=True)


def process_post_votes(post_id, post_created):
    try:
        headers = {
            'X-Futuware-UID': os.environ['UID'],
            'X-Futuware-SID': os.environ['SID']
        }

        page = 1
        url = f'https://d3.ru/api/posts/{post_id}/votes/?per_page=210&page={page}'
        response = requests.get(url, headers=headers, timeout=30).json()

        if 'status' in response and response['status'] == 'error':
            print('❌', response)
            return

        while response['upvotes'] is not None and response['downvotes'] is not None:
            if response['upvotes'] is not None:
                for vote in response['upvotes']:
                    upsert_post_vote(post_id, post_created, vote)
            if response['downvotes'] is not None:
                for vote in response['downvotes']:
                    upsert_post_vote(post_id, post_created, vote)

            if response['page'] == response['page_count']:
                break

            page += 1
            url = f'https://d3.ru/api/posts/{post_id}/votes/?per_page=210&page={page}'
            response = requests.get(url, headers=headers, timeout=30).json()
    except Exception as e:  # pylint: disable=broad-exception-caught
        traceback_str = traceback.format_exc()
        print('❌', e, traceback_str)


def process_comment_votes(post_id, comment_id, comment_created):
    try:
        headers = {
            'X-Futuware-UID': os.environ['UID'],
            'X-Futuware-SID': os.environ['SID']
        }

        page = 1
        url = f'https://d3.ru/api/comments/{comment_id}/votes/?per_page=210&page={page}'
        response = requests.get(url, headers=headers, timeout=30).json()

        if 'status' in response and response['status'] == 'error':
            print('❌', response)
            return

        while response['upvotes'] is not None and response['downvotes'] is not None:
            if response['upvotes'] is not None:
                for vote in response['upvotes']:
                    upsert_comment_vote(post_id, comment_id,
                                        comment_created, vote)
            if response['downvotes'] is not None:
                for vote in response['downvotes']:
                    upsert_comment_vote(post_id, comment_id,
                                        comment_created, vote)

            if response['page'] == response['page_count']:
                break

            page += 1
            url = f'https://d3.ru/api/comments/{comment_id}/votes/?per_page=210&page={page}'
            response = requests.get(url, headers=headers, timeout=30).json()
    except Exception as e:  # pylint: disable=broad-exception-caught
        traceback_str = traceback.format_exc()
        print('❌', e, traceback_str)
