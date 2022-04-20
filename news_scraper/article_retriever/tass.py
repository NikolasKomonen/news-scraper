import asyncio
from dataclasses import dataclass
from functools import lru_cache
import logging
import re
import sys
from time import sleep
from typing import Iterable
from playwright.sync_api import sync_playwright
from yarl import URL
from news_scraper.article.article import DefaultArticleMetadata, DefaultArticleText

from news_scraper.article.protocols import ArticleText
from datetime import datetime

logger = logging.getLogger()

"""
Certain articles are bad, the following with the path in them
we should skip:

/sport/
/business-officials/
/top-officials/

Query:
db.article_scraped_urls.updateMany({}, {$pull: {'scraped_urls': {"$regex": /.*\/sport\/.*/}}})


"""

URLS_TO_SKIP = re.compile(r".+\/((sport)|(business\-officials)|(top\-officials))\/.+")

TIMEOUT_MILLISECONDS = 60000


@dataclass(frozen=True)
class TassArticleRetriever:
    def __call__(self, url: URL) -> ArticleText:
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page()
            logger.info(f"Loading page: '{url.human_repr()}'")
            page.goto(url.human_repr(), timeout=TIMEOUT_MILLISECONDS)

            # Get the HTML <div> that holds the article
            
            article_div = page.locator(".news")

            article_div.wait_for(timeout=TIMEOUT_MILLISECONDS)

            # Get the unix time when the article was created
            datetime_int = int(
                article_div.locator("dateformat").first.get_attribute("time")
            )
            assert isinstance(datetime_int, int)
            datetime_obj = datetime.fromtimestamp(datetime_int)
            metadata = DefaultArticleMetadata(datetime=datetime_obj, url=url)

            # Get the different pieces of text in the article
            title = article_div.locator(".news-header__title").inner_text()
            lead = article_div.locator(".news-header__lead").inner_text()
            body = article_div.locator(".text-content").all_inner_texts()
            body = re.sub("\n", " ", re.sub("\n\n", "\n", "".join(body)))

            return DefaultArticleText(
                title=title, lead=lead, body=body, metadata=metadata
            )


@dataclass(frozen=True)
class TassArticleUrlScraper:
    """Scrapes the HTML of a Tass webpage for links to articles."""

    href_pattern = re.compile(r"\/[a-z]+\/\d+")

    def __call__(self, url: URL) -> Iterable[URL]:
        assert "tass" in url.human_repr()
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page()
            logger.info(f"Loading page for '{url.human_repr()}'")
            page.goto(url.human_repr(), timeout=90000)

            # This forces the code to wait for the
            # page to actually load, since tass needs
            # to run js script which loads the ACTUAL content.
            page.locator("main.container").wait_for(timeout=90000)

            results = set()
            a_tags = page.locator("main.container").locator("a")
            for tag_index in range(a_tags.count()):
                href = a_tags.nth(tag_index).get_attribute("href")
                if (
                    self.href_pattern.match(href) is not None
                    and URLS_TO_SKIP.match(href) is None
                ):
                    results.add(URL.build(scheme=url.scheme, host=url.host, path=href))
            return results


cached_tass_article_retriever = lru_cache()(TassArticleRetriever())
cached_tass_article_url_scraper = lru_cache()(TassArticleUrlScraper())
