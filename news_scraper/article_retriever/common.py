from dataclasses import dataclass
import logging
from sqlite3 import Connection
import sys
from typing import Optional
import pandas
from yarl import URL
from news_scraper.article_retriever.database import (
    DatabaseArticleRetriever,
    DatabaseArticleUrlScraper,
)
from news_scraper.article_retriever.protocols import ArticleRetriever, ArticleUrlScraper
from news_scraper.article_retriever.tass import (
    cached_tass_article_retriever,
    cached_tass_article_url_scraper,
)
from news_scraper.article_retriever.wayback import (
    WayBackMachineArticleUrlScraper,
    cached_wayback_machine_article_retriever,
)
from pymongo.database import Database

from news_scraper.database.mongo.client import NEWS_SCRAPER_DATABASE, Mongo

logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
logger.addHandler(handler)


@dataclass(frozen=True)
class CachedArticleRetrieverResolver:
    """This is a top level class that gives you the
    correct article retriever for the given url.

    Any new article retrievers will most likely want
    to be added in to here.

    This class abstracts the need to figure out which
    retriever is needed.

    Also all retrievers should use a cache, since this
    is the goal of this implementation.
    """

    database: Database = None
    tass_retriever: ArticleRetriever = cached_tass_article_retriever
    wayback_retriever: ArticleRetriever = cached_wayback_machine_article_retriever

    def __call__(self, url: URL):
        """"""
        host = url.host
        if not host:
            raise Exception(
                f"Could not determine an ArticleRetriever for '{url.human_repr()}'."
            )
        retriever = None
        if "web.archive" in host:
            retriever = self.wayback_retriever
            if self.database is not None:
                return DatabaseArticleRetriever(self.database, retriever)
            return retriever
        raise Exception(
            f"Could not determine an ArticleRetriever for '{url.human_repr()}'."
        )


@dataclass(frozen=True)
class DefaultArticleUrlScraperResolver:
    """This is a top level class that gives you the
    correct article scraper for the given url.

    """

    connection: Optional[Connection] = None
    wayback_scraper: ArticleUrlScraper = WayBackMachineArticleUrlScraper()

    def __call__(self, url: URL) -> ArticleUrlScraper:
        """"""
        host = url.host
        if not host:
            raise Exception(
                f"Could not determine an ArticleRetriever for '{url.human_repr()}'."
            )
        resolved_scraper = None
        if "web.archive" in host:
            resolved_scraper = self.wayback_scraper

            if self.connection is not None:
                resolved_scraper = DatabaseArticleUrlScraper(
                    self.connection, resolved_scraper
                )
            return resolved_scraper
        raise Exception(
            f"Could not determine an ArticleRetriever for '{url.human_repr()}'."
        )


def scrape_all():
    mongo = Mongo()
    db = mongo.get_database(NEWS_SCRAPER_DATABASE)
    article_scraper_resolver = DefaultArticleUrlScraperResolver(db)

    df = pandas.read_csv("all_inputs/wayback_links.csv", header=0)
    total_rows = len(df)
    counter = 1
    under_20 = []
    for _, row in df.iterrows():
        page_url = row["wayback_url"]
        if pandas.isna(page_url):
            logger.info("----------")
            logger.info("[WARNING]: Skipped row since no URL")
            counter += 1
            continue
        # page_timestamp = ["wayback_ts"]
        article_scraper = article_scraper_resolver(URL(page_url))
        urls = article_scraper(URL(page_url))
        if len(urls) < 20:
            under_20.append(page_url)
        logger.info(f"Total Scraped: {counter}/{total_rows}")
        counter += 1
    print(under_20)
