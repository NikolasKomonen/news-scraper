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

from news_scraper.url.common import resolve_hyperlink


logger = logging.getLogger()


@dataclass(frozen=True)
class LentaArticleRetriever:
    def __call__(self, url: URL) -> ArticleText:
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page()
            page.goto(url.human_repr(), timeout=90000)

            # Get the HTML <div> that holds the article
            logger.info(f"Waiting for page to load for {url.human_repr()}")
            article_div = page.locator(".topic-page__container")
            article_div.wait_for(timeout=90000)
            logger.info("Done.")

            # Get the unix time when the article was created
            datetime_str = article_div.locator("topic-header__time").first.inner_text()

            assert isinstance(datetime_str, str)
            datetime_obj = datetime.strftime(datetime_str, "%H:%M, %B %Y")
            metadata = DefaultArticleMetadata(datetime=datetime_obj, url=url)

            # Get the different pieces of text in the article
            title = article_div.locator(".article__header h1").inner_text()
            lead = article_div.locator(".article__header h2").inner_text()
            body = article_div.locator(".article__content p").all_inner_texts()
            body = re.sub("\n", " ", re.sub("\n\n", "\n", " ".join(body)))

            return DefaultArticleText(
                title=title, lead=lead, body=body, metadata=metadata
            )


@dataclass(frozen=True)
class LentaArticleUrlScraper:
    """Scrapes the HTML of a Tass webpage for links to articles."""

    href_pattern = re.compile(r"\/[^ ]+\/\d+\/\d+\/\d+\/[^ ]+\/")

    def __call__(self, url: URL) -> Iterable[URL]:
        assert "lenta" in url.human_repr()
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page()
            logger.info(f"Loading page for '{url.human_repr()}'")
            page.goto(url.human_repr(), timeout=90000)

            # This forces the code to wait for the
            # page to actually load, since tass needs
            # to run js script which loads the ACTUAL content.
            container = page.locator("div.main-page")
            container.wait_for(timeout=90000)

            a_tags = container.locator("a")
            result = set()
            for tag_index in range(a_tags.count()):
                href = a_tags.nth(tag_index).get_attribute("href")
                if self.href_pattern.match(href) is not None:
                    result.add(resolve_hyperlink(url, href))
            return result
