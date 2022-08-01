from dataclasses import dataclass

import logging
from sqlite3 import Connection
from typing import Iterable
from yarl import URL

from news_scraper.article_retriever.protocols import ArticleRetriever, ArticleUrlScraper
from news_scraper.article_retriever.wayback import WAYBACK_ARCHIVE_PATTERN
from news_scraper.database.sqlite.client import (
    ArticleMetadataTable,
    ArticleTextTable,
    ScrapedWaybackUrl,
    WaybackResult,
)

from ..article.protocols import ArticleText

logger = logging.getLogger()


class DatabaseArticleException(Exception):
    """"""


@dataclass(frozen=True)
class DatabaseArticleRetriever:
    """
    Article Retriever that uses a database
    to cache results and will load results
    from cache if they already exist.
    """

    connection: Connection
    actual_retriever: ArticleRetriever

    def __call__(self, url: URL) -> ArticleText:

        att = ArticleTextTable(self.connection)

        match = WAYBACK_ARCHIVE_PATTERN.match(url.human_repr())
        if match is not None:
            article = att.get_article(URL(match.group("URL")))
        else:
            article = att.get_article(url)

        if article is not None:
            logger.info(f"Found cached text for: '{url.human_repr()}'")
            return article

        # Fetch the aritcle from website, then save to db
        logger.info(f"Fetching text for: '{url.human_repr()}'.")
        article_text = self.actual_retriever(url)
        assert isinstance(article_text, ArticleText)

        att.add_article(article_text)
        # collection.insert_one(document_to_insert)
        logger.info(f"Cached text for: '{url.human_repr()}'.")
        return article_text


@dataclass(frozen=True)
class DatabaseArticleUrlScraper:

    connection: Connection
    article_scraper: ArticleUrlScraper

    def __call__(self, url: URL) -> Iterable[URL]:
        wr = WaybackResult(self.connection)
        wr_id = wr._get_id_from_wayback_url(url)

        if wr_id == 0:
            raise DatabaseArticleException(
                f"No entry in table {wr.name} for url '{url.human_repr()}'"
            )

        swu = ScrapedWaybackUrl(self.connection)
        scraped_url_count = swu.get_count_for_id(wr_id)

        if scraped_url_count == 0:
            # Actually scrape the webpage with the given scraper, then save to db
            logger.info(
                f"Article to scrape urls from, '{url.human_repr()}', was not cached."
            )
            urls = set(self.article_scraper(url))

            swu.add_urls(wr_id, urls)
            logger.info(f"Cached '{len(urls)}' scraped urls for '{url.human_repr()}'.")
            return urls
        else:
            # Load article from db
            urls = list(swu.get_urls_with_id(wr_id))
            logger.info(
                f"Loading '{len(urls)}' cached urls scraped from '{url.human_repr()}'."
            )
            return urls
