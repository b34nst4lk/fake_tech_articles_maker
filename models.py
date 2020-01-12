from dataclasses import dataclass
from datetime import datetime
import sqlite3


def parse_datetime(dt):
    date, time = dt.split("T")
    year, month, day = [int(i) for i in date.split("-")]
    time = time.replace("Z", "")
    hour, minute, second = [int(i) for i in time.split(":")]
    return datetime(year, month, day, hour, minute, second)


class DB:
    def __init__(self, db_objects):
        self.conn = sqlite3.connect("test.db")
        self.db_objects = tuple(db_objects)
        for db_object in db_objects:
            db_object.create_table(self.conn)

    def insert(self, data):
        if isinstance(data, self.db_objects):
            type(data).insert(data, self.conn)
        elif isinstance(data, list) and len(data) > 0 and isinstance(data[0], DBObject):
            type(data[0]).insert_many(data, self.conn)


class DBObject:
    @classmethod
    def create_table(cls, conn):
        assert cls.create_table_sql_statement is not None
        c = conn.cursor()
        c.execute(cls.create_table_sql_statement)

    @classmethod
    def from_dict(cls, input_dict):
        kwargs = {}
        for var_name in cls.__annotations__:
            if var_name in input_dict:
                kwargs[var_name] = input_dict[var_name]

        return cls(**kwargs)

    def to_tuple(self):
        return tuple([i for i in self.__dict__.values()])

    @classmethod
    def insert_many(cls, data, conn):
        if not data:
            return
        elif isinstance(data, cls):
            data = [data]
        elif not isinstance(data, list):
            raise TypeError(f"Only list and {cls.__name__} instances allowed")

        tupled_data = []
        for datum in data:
            if cls.check_if_exists(datum, conn) is False:
                cls.sanitize(datum)
                tupled_data.append(datum.to_tuple())

        insert_statement = cls.make_insert_statement()

        c = conn.cursor()
        c.executemany(insert_statement, tupled_data)
        conn.commit()
        c.close()

    @classmethod
    def insert(cls, data, conn):
        if not data:
            return
        elif cls.check_if_exists(data, conn):
            return
        elif not isinstance(data, cls):
            raise TypeError(f"Only {cls.__name__} instances allowed")

        data = data.to_tuple()

        insert_statement = cls.make_insert_statement()

        c = conn.cursor()
        c.execute(insert_statement, data)
        conn.commit()
        c.close()

    @classmethod
    def make_insert_statement(cls):
        columns = "," .join(cls.__annotations__)
        values = ",".join(["?" for i in cls.__annotations__])
        return f"INSERT INTO {cls.table_name} ({columns}) VALUES ({values})"

    @classmethod
    def check_if_exists(cls, data, conn):
        if hasattr(cls, "unique") is False:
            return False

        select_statement = f"""
            SELECT 1 FROM {cls.table_name} WHERE {cls.unique} = ?
        """
        value = tuple([data.__dict__.get(cls.unique)])

        c = conn.cursor()
        c.execute(select_statement, (value))
        entry = c.fetchone()
        c.close()

        return entry is not None

    @classmethod
    def sanitize(cls, obj):
        for key, field_type in cls.__annotations__.items():
            value = obj.__dict__.get(key),
            if field_type is datetime and isinstance(value, str):
                obj.key = parse_datetime(obj.__dict__[key])
            pass


@dataclass
class User(DBObject):
    username: str = None
    name: str = None
    twitter_user_name: str = None
    github_user_name: str = None
    website_url: str = None
    profile_image: str = None
    profile_image_90: str = None

    unique = "username"

    table_name = "users"
    create_table_sql_statement = f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        (
            username            TEXT    UNIQUE,
            name                TEXT,
            twitter_user_name   TEXT,
            github_user_name    TEXT,
            website_url         TEXT,
            profile_image       TEXT,
            profile_image_90    TEXT
        )
    """


@dataclass
class Organization(DBObject):
    username: str = None
    name: str = None
    slug: str = None
    profile_image: str = None
    profile_image_90: str = None

    unique = "username"

    table_name = "organizations"
    create_table_sql_statement = f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        (
            username            TEXT    UNIQUE,
            name                TEXT,
            slug                TEXT,
            profile_image       TEXT,
            profile_image_90    TEXT
        )
    """


@dataclass
class ArticleHeader(DBObject):
    type_of: str = None
    id: int = None
    title: str = None
    description: str = None
    cover_image: str = None
    readable_publish_date: str = None
    social_image: str = None
    tags: str = None
    slug: str = None
    path: str = None
    url: str = None
    canonical_url: str = None
    comments: int = None
    positive_reactions_count: int = None
    collection_id: int = None
    comments_count: int = None
    created_at: datetime = None
    edited_at: datetime = None
    crossposted_at: datetime = None
    published_at: datetime = None
    last_comment_at: datetime = None
    published_timestamp: datetime = None
    username: str = None
    org_name: str = None

    unique = "id"

    table_name = "article_headers"
    create_table_sql_statement = f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        (
            id                          INTEGER    UNIQUE,
            type_of                     TEXT,
            title                       TEXT,
            description                 TEXT,
            cover_image		        TEXT,
            readable_publish_date       TEXT,
            social_image                TEXT,
            tags                        TEXT,
            slug		        TEXT,
            path		        TEXT,
            url		                TEXT,
            canonical_url               TEXT,
            comments		        INTEGER,
            positive_reactions_count	INTEGER,
            collection_id		INTEGER,
            comments_count		INTEGER,
            created_at		        TIMESTAMP,
            edited_at		        TIMESTAMP,
            crossposted_at		TIMESTAMP,
            published_at		TIMESTAMP,
            last_comment_at		TIMESTAMP,
            published_timestamp		TIMESTAMP,
            username		        TEXT,
            org_name		        TEXT,

            CONSTRAINT fk_user_of_article
                FOREIGN KEY (username)
                REFERENCES users (username)
            CONSTRAINT fk_org_of_article
                FOREIGN KEY (org_name)
                REFERENCES organizations (username)
        )
    """


@dataclass
class ArticleBody(DBObject):
    id: int = None
    body_html: str = None
    body_markdown: str = None

    unique = "id"

    table_name = "article_bodies"
    create_table_sql_statement = f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        (
            id      INTEGER    UNQIUE,
            body_html       TEXT,
            body_markdown   TEXT,

            CONSTRAINT fk_article_body_of_header
                FOREIGN KEY (id)
                REFERENCES article_headers (id)
        )
    """


if __name__ == "__main__":
    conn = sqlite3.connect("test.db")
    import pdb;pdb.set_trace()
