import json
import requests
import sqlite3
from time import sleep
from models import User, Organization, ArticleHeader, ArticleBody, DB

articles_url = "https://dev.to/api/articles"


def get_articles_headers(page=1):
    articles_response = requests.get(articles_url, params={"page": page})
    if articles_response.status_code == 200:
        return json.loads(articles_response.content)
    else:
        error_code = articles_response.status_code
        reason = articles_response.reason
        raise ConnectionError(f"{error_code}: {reason}")


def get_article(article_id):
    article_response = requests.get(articles_url + f"/{article_id}")
    if article_response.status_code == 200:
        return json.loads(article_response.content)
    else:
        error_code = article_response.status_code
        reason = article_response.reason
        raise ConnectionError(f"{error_code}: {reason}")



def flatten_article_header(article):
    article.pop("tag_list")
    user = article.pop("user")
    org = article.pop("organization") if "organization" in user else None

    user_dao = None
    article["username"] = None
    if user:
        article["username"] = user.get("username")
        user_dao = User.from_dict(user)

    org_dao = None
    article["org_name"] = None
    if org:
        article["org_name"] = org.get("org_name")
        org_dao = Organization.from_dict(org)

    article_header = ArticleHeader.from_dict(article)
    return article_header, user_dao, org_dao


def save_article_headers(db, article_headers, article_bodies, users, orgs):
    db.insert(users)
    db.insert(orgs)
    db.insert(article_headers)
    db.insert(article_bodies)


def main():
    db = DB([User, Organization, ArticleHeader, ArticleBody])
    still_scraping = True
    page_number = 1
    while still_scraping:
        articles = get_articles_headers(page_number)
        if not articles:
            break
        article_headers = []
        users, orgs = {}, {}

        article_ids = []

        for article in articles:
            article_ids.append(article.get("id"))

            article_header, user, org = flatten_article_header(article)
            article_headers.append(article_header)
            if user:
                users[user.username] = user
            if org:
                orgs[user.username] = org

        users = [user for user in users.values()]
        orgs = [org for org in orgs.values()]

        articles = []
        for article_id in article_ids:
            test_article = ArticleBody(id=article_id)
            if not ArticleBody.check_if_exists(test_article, db.conn):
                print(page_number, article_id)
                article = get_article(article_id)
                article = ArticleBody.from_dict(article)
                articles.append(article)
                sleep(.2)

        save_article_headers(db, article_headers, articles, users, orgs)
        page_number += 1


if __name__ == "__main__":
    main()
