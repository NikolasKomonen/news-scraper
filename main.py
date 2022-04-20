import datetime
import logging
import sys
from typing import Iterable
import pandas

from yarl import URL
from news_scraper.article.article import DefaultArticleMetadata, DefaultArticleText
from news_scraper.article.protocols import ArticleMetadata
from news_scraper.article_retriever.common import (
    DefaultArticleRetrieverResolver,
    DefaultArticleUrlScraperResolver,
)
from playwright._impl._api_types import TimeoutError

from news_scraper.article_finder.wayback_machine import do
from news_scraper.database.sqlite.client import (
    ArticleTextTable,
    NewsScraperSqliteConnection,
    NewsWebsiteEnum,
    ScrapedWaybackUrl,
    WaybackResult,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def scrape_articles():
    with NewsScraperSqliteConnection.connection() as conn:
        wr = WaybackResult(conn)
        urls = wr.get_urls()

        for url in urls:
            if "lenta.ru" in url.human_repr():
                continue
            logger.info("----------")
            resolver = DefaultArticleUrlScraperResolver(conn)
            scraper = resolver(url)
            scraper(url)


def add_articles():
    metadata = DefaultArticleMetadata(url=URL("https://tass.com/article2"), datetime=datetime.datetime.utcnow())
    article_text = DefaultArticleText("My Title Updated", "My Lead", "My Body", metadata)
    with NewsScraperSqliteConnection.connection() as conn:
        ArticleTextTable(conn).add_article(article_text)


def get_tass_article_text():
    
    with NewsScraperSqliteConnection.connection() as conn:
        swu = ScrapedWaybackUrl(conn)
        urls = swu.get_unique_urls_from_site(NewsWebsiteEnum.TASS)
        
        scraper = DefaultArticleRetrieverResolver(conn)(URL.build(scheme="https", host="web.archive", path=f"/{NewsWebsiteEnum.TASS.value}"))
        for url in urls:
            print("----------")
            try:
                scraper(url)
            except Exception as e:
                logger.info("Retrying after exception was raised.")
                logger.exception(e)
                try:
                    scraper(url)
                except Exception:
                    logger.info(f"Skipping {url.human_repr()} since it fails.")


get_tass_article_text()

