#!/usr/bin/env python
# coding=utf8

"""
Coral Talk data migration script from coral talk v4 to v6.
la diaria 2020. ladiaria.com.uy
Aníbal Pacheco. apacheco@ladiaria.com.uy
"""

from uuid import uuid4
from pprint import pprint
from progress.bar import Bar

from settings import DEBUG, TENNANT_ID, SITE_ID
from helpers import v4db, v6db, user_id_map, story_id_map


assert TENNANT_ID, 'TENNANT_ID setting should be configured.'
assert SITE_ID, 'SITE_ID setting should be configured.'


def migrate_user(user_id, update=False, print_only=False):
    """
    user_id is the id of the user in v4 databse.
    update indicates if the data should be updated in case that this user was already migrated.
    if print_only is True nothing will be saved and the data will be printed only, if DEBUG is True.
    """
    v4user = v4db.users.find_one({'id': user_id})
    assert v4user, 'User %s not found in source database' % user_id

    v6user_id, v6username = user_id_map(user_id)

    assert v6user_id, 'User %s could not be mapped to v6.' % user_id

    v6user = v6db.users.find_one({'id': v6user_id})
    if not v6user or update:
        v6user = {
            'tenantID': TENNANT_ID, 'email': user_id, 'username': v6username, 'createdAt': v4user['created_at'],
            'role': v4user['role'],
            'status': {
                'username': {'history': []}, 'suspension': {'history': []}, 'ban': {'active': False, 'history': []},
                'premod': {'active': False, 'history': []}},
            'notifications': {
                'onReply': False, 'onFeatured': False, 'onModeration': False, 'onStaffReplies': False,
                'digestFrequency': 'NONE'}}
        profiles = [{'type': 'sso', 'id': v6user_id, 'lastIssuedAt': v4user['created_at']}]
        if v4user['role'] == 'MODERATOR':
            profiles.append({'type': 'local', 'id': user_id})
            v6user['emailVerified'] = True
            v6user['status']['username']['history'] = [{
                'id': str(uuid4()), 'username': v6username, 'createdBy': v6user_id,
                'createdAt': v4user['status']['username']['history'][0]['created_at']}]
        v6user['profiles'] = profiles
        if DEBUG:
            print('\nDEBUG: migrating user "%s" to v6 with the following data:' % user_id)
            pprint(v6user)
        if not print_only:
            v6db.users.update_one({'id': v6user_id}, {'$set': v6user}, upsert=True)


def inc_comment_count(story_id):
    v6db.stories.update_one({'id': story_id}, {'$inc': {'commentCounts.status.APPROVED': 1}})


def comment_migrated(comment_id):
    return bool(v6db.comments.find_one({'id': comment_id}))


def migrate_comment(comment_id, parent_id=None, print_only=False):
    """
    Migrates a comment, if parent_id has value the comment will be migrated as a child comment of thah parent.
    if print_only is True nothing will be saved and the data will be printed only, if DEBUG is True.
    Precondition: the comment story should have been migrated before.
    """
    if comment_migrated(comment_id):
        if DEBUG:
            print('DEBUG: Comment already migrated.')
    else:
        if parent_id and not comment_migrated(parent_id):
            print('Parent comment not migrated yet, skipping.')
            return

        v4comment = v4db.comments.find_one({'id': comment_id})

        v6author_id = user_id_map(v4comment['author_id'])[0]
        assert v6author_id, 'Author could not be mapped to v6, skipping.'

        v6comment = {
            'id': comment_id, 'tenantID': TENNANT_ID, 'childIDs': [], 'childCount': 0, 'revisions': [{
                'id': str(uuid4()), 'body': '<div>%s</div>' % v4comment['body'], 'actionCounts': {},
                'metadata': {'nudge': True, 'linkCount': 0}, 'createdAt': v4comment['created_at']
            }], 'createdAt': v4comment['created_at'], 'storyID': story_id_map(v4comment['asset_id'])[0],
            'authorID': v6author_id, 'siteID': SITE_ID, 'tags': [], 'status': "NONE", 'ancestorIDs': [],
            'actionCounts': {}
        }

        if parent_id:
            v6parent = v6db.comments.find_one({'id': parent_id}) if DEBUG and print_only else \
                v6db.comments.find_one_and_update(
                    {'id': parent_id}, {'$push': {'childIDs': comment_id}, '$inc': {'childCount': 1}})

            v6comment.update({
                'parentID': parent_id, 'parentRevisionID': v6parent['revisions'][0]['id'],
                'ancestorIDs': [parent_id]})

        if DEBUG:
            print('\nDEBUG: Migrating comment to v6 with the following data:')
            pprint(v6comment)

        if not print_only:
            if not parent_id:
                inc_comment_count(v6comment['storyID'])
            v6db.comments.insert_one(v6comment)


def migrate_story(asset_id, print_only=False):
    """
    if print_only is True nothing will be saved and the data will be printed only, if DEBUG is True.
    """
    asset = v4db.assets.find_one({'id': asset_id})
    assert asset, 'v4 asset not found.'

    story_id, story_url = story_id_map(asset_id)

    if not v6db.stories.find_one({'id': story_id}):
        story = {
            'tenantID': TENNANT_ID, 'url': story_url,
            'commentCounts': {'action': {}, 'status': {'APPROVED': 0, 'NONE': 0}, 'moderationQueue': {'total': 0}},
            'createdAt': asset['created_at'], 'id': story_id, 'settings': {}, 'siteID': SITE_ID, 'closedAt': False}
        if DEBUG:
            print('\nDEBUG: Migrating asset "%s" to a v6 story with the following data:' % asset_id)
            pprint(story)
        if not print_only:
            v6db.stories.insert_one(story)

    elif DEBUG:
        print('\nDEBUG: Story %s already migrated.' % story_id)


def migrate(print_only=False):
    if DEBUG:
        print('DEBUG: Migrating all parent (root level) comments to v6 ...')

    v4comments = v4db.comments.find({'status': "ACCEPTED", 'parent_id': None}, no_cursor_timeout=True)

    bar, not_migrated = Bar('Migrating ...', max=v4comments.count()), set()

    for v4comment in v4comments:
        v4comment_id = v4comment['id']
        if not comment_migrated(v4comment_id):
            try:
                migrate_user(v4comment['author_id'], print_only=print_only)
            except Exception:
                not_migrated.add(v4comment_id)
                print('\nERROR: Author of comment %s could not be migrated, skipping.' % v4comment_id)
                bar.next()
                continue

            try:
                migrate_story(v4comment['asset_id'], print_only=print_only)
            except Exception:
                not_migrated.add(v4comment_id)
                print('\nERROR: Story of comment %s could not be migrated, skipping.' % v4comment_id)
                bar.next()
                continue

            try:
                migrate_comment(v4comment_id, print_only=print_only)
            except Exception:
                not_migrated.add(v4comment_id)
                print('\nERROR: Comment %s could not be migrated, skipping.' % v4comment_id)
                bar.next()
                continue

        elif DEBUG:
            print('\nDEBUG: Comment %s already migrated.' % v4comment_id)

        bar.next()

    bar.finish()

    if DEBUG:
        print('\nDEBUG: Migrating all child comments to v6 ...')

    v4comments = v4db.comments.find({'status': "ACCEPTED", 'parent_id': {'$ne': None}}, no_cursor_timeout=True)

    children = [v4comment['id'] for v4comment in v4comments]

    while children:

        if DEBUG:
            print('DEBUG: %d children left.' % len(children))

        for v4comment in v4db.comments.find({
                'status': "ACCEPTED", 'parent_id': {'$ne': None}}, no_cursor_timeout=True):

            v4comment_id = v4comment['id']
            if not comment_migrated(v4comment_id):

                parent_id = v4comment['parent_id']

                if parent_id in not_migrated:
                    not_migrated.add(v4comment_id)
                else:
                    if comment_migrated(parent_id):

                        try:
                            migrate_user(v4comment['author_id'], print_only=print_only)
                        except Exception:
                            not_migrated.add(v4comment_id)
                            if v4comment_id in children:
                                children.remove(v4comment_id)
                            print('ERROR: Author of comment %s could not be migrated, skipping.' % v4comment_id)
                            continue

                        try:
                            migrate_comment(v4comment_id, parent_id, print_only=print_only)
                        except Exception:
                            not_migrated.add(v4comment_id)
                            print('ERROR: Comment %s could not be migrated, skipping.' % v4comment_id)

                    else:
                        if not v4db.comments.find_one({'id': parent_id, 'status': 'ACCEPTED'}):
                            # If parent is not accepted this child will not be migrated
                            not_migrated.add(v4comment_id)
                        else:
                            # Also considering if print_only is True the parent will be never migrated
                            if print_only:
                                print('Parent comment "%s" not migrated (print_only mode), skipping.' % parent_id)
                            else:
                                continue

            if v4comment_id in children:
                children.remove(v4comment_id)

    if not_migrated:
        print('Comments not migrated: %s' % not_migrated)


if __name__ == "__main__":
    migrate()
