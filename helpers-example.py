from pymongo import MongoClient
from urlparse import urlsplit
from requests import head

from django.contrib.auth.models import User

from core.models import Article, ArticleUrlHistory


client = MongoClient()
v4db = client['talk']
v6db = client['coral']


def user_id_map(user_id):
    try:
        u = User.objects.get(email=user_id)
        return (str(u.id), u.get_full_name() or u.username)
    except Exception:
        return (None, None)


def story_id_map(asset_id):
    asset = v4db.assets.find_one({'id': asset_id})
    assert asset, 'v4 asset not found.'

    url_path = urlsplit(head(asset['url']).headers.get('location', asset['url'])).path

    try:
        article = Article.objects.get(url_path=url_path)
    except Article.DoesNotExist:
        article = ArticleUrlHistory.objects.get(absolute_url=url_path).article

    return (str(article.id), 'https://hexxie.com' + article.url_path)
