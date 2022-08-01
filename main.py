import datetime
import logging
import sys
from typing import Iterable
import pandas

from yarl import URL
from news_scraper.article.article import DefaultArticleMetadata, DefaultArticleText
from news_scraper.article.protocols import ArticleMetadata

from playwright._impl._api_types import TimeoutError

from news_scraper.article_finder.wayback_machine import do
from news_scraper.article_retriever.database import DatabaseArticleRetriever
from news_scraper.article_retriever.wayback import (
    WAYBACK_ARCHIVE_PATTERN,
    WayBackMachineArticleRetriever,
)
from news_scraper.database.sqlite.client import (
    AliasOfRootWordTable,
    ArticleTextTable,
    NewsScraperSqliteConnection,
    NewsWebsiteEnum,
    RootWordTable,
    ScrapedWaybackUrl,
    WaybackResult,
)
from news_scraper.word.load import load_all_text_with_aliases
from news_scraper.word.save import SqlSaveTextWithAliases
from news_scraper.word.word import DefaultTextWithAliases

logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)


def add_articles():
    metadata = DefaultArticleMetadata(
        url=URL("https://tass.com/article2"), datetime=datetime.datetime.utcnow()
    )
    article_text = DefaultArticleText(
        "My Title Updated", "My Lead", "My Body", metadata
    )
    with NewsScraperSqliteConnection.connection() as conn:
        ArticleTextTable(conn).add_article(article_text)


def get_mt_article_text():

    with NewsScraperSqliteConnection.connection() as conn:
        swu = ScrapedWaybackUrl(conn)
        urls = list(swu.get_unique_urls_from_site(NewsWebsiteEnum.RIA))
        print(len(urls))

        cached_wayback_retriever = DatabaseArticleRetriever(
            conn, WayBackMachineArticleRetriever()
        )
        for url in urls:
            print("----------")
            try:
                cached_wayback_retriever(url)
            except Exception as e:
                logger.warning(f"Failed retrieving for {url.human_repr()}.")


def update_scraped_urls():
    with NewsScraperSqliteConnection.connection() as conn:
        swu = ScrapedWaybackUrl(conn)
        query = f"""\
            SELECT wayback_result_id, scraped_url
            FROM scraped_wayback_url
        """
        rows = list(swu.query(query))

        for row in rows:
            wayback_id = row[0]
            scraped_url = row[1]
            match = WAYBACK_ARCHIVE_PATTERN.match(scraped_url)
            try:
                assert match is not None
            except AssertionError as e:
                print(scraped_url)
            actual_url = match.group("URL")
            transaction = f"""
                INSERT INTO scraped_wayback_url
                VALUES ('{wayback_id}', '{scraped_url}', '{actual_url}')
                ON CONFLICT DO UPDATE SET actual_url='{actual_url}'
            """
            swu.connection.execute(transaction)
        swu.connection.commit()


def load_all_word_variants_to_db():
    """
    This is an idempotent function that adds all
    word variants to the DB.

    You can add more instances to the list
    and safely run this function again to add any
    new values.
    """

    to_add = [
        DefaultTextWithAliases(root="covid", aliases=["covid19"]),
        DefaultTextWithAliases(root="vaccine", aliases=["vax", "vaxx"]),
        DefaultTextWithAliases(root="putin", aliases=[]),
        DefaultTextWithAliases(root="biden", aliases=[]),
        DefaultTextWithAliases(root="president", aliases=[]),
        DefaultTextWithAliases(root="qr", aliases=[]),
        DefaultTextWithAliases(root="sputnik", aliases=[]),
        DefaultTextWithAliases(root="inoculate", aliases=["innoculation"]),
        DefaultTextWithAliases(root="vaccinate", aliases=["vaccination"]),
        DefaultTextWithAliases(root="omicron", aliases=[]),
        DefaultTextWithAliases(root="pfizer", aliases=[]),
        DefaultTextWithAliases(root="moderna", aliases=[]),
        DefaultTextWithAliases(root="j&j", aliases=["johnson johnson"]),
        DefaultTextWithAliases(root="pandemic", aliases=[]),
        DefaultTextWithAliases(root="russia", aliases=[]),
        DefaultTextWithAliases(root="moscow", aliases=[]),
        DefaultTextWithAliases(root="Elf", aliases=["OnAShelf"]),
        DefaultTextWithAliases(root="america", aliases=["american"]),
    ]

    with NewsScraperSqliteConnection.connection() as conn:
        saver = SqlSaveTextWithAliases(RootWordTable(conn), AliasOfRootWordTable(conn))
        for twa in to_add:
            saver(twa)
