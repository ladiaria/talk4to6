from pymongo import MongoClient

client = MongoClient()
v4db = client['talk']
v6db = client['coral']


def user_id_map(user_id):
    """
    This function should return a tuple with the new id and username for the migrated user.
    """
    return (user_id, user_id)


def story_id_map(asset_id):
    """
    This function should return a tuple with the new id and url for the migrated story.
    """
    return (asset_id, v4db.assets.find({'id': asset_id})['url'])
