import asyncio
from dataclasses import dataclass
from functools import lru_cache
import locale
import logging
import re
import sys
from time import sleep
from typing import Iterable
import arrow
from playwright.sync_api import sync_playwright
import pytz
from yarl import URL
from news_scraper.article.article import DefaultArticleMetadata, DefaultArticleText

from news_scraper.article.protocols import ArticleText
from datetime import datetime
from arrow import locales

logger = logging.getLogger()


@dataclass(frozen=True)
class RiaArticleRetriever:
    def __call__(self, url: URL) -> ArticleText:
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page()
            page.goto(url.human_repr(), timeout=90000)

            # Get the HTML <div> that holds the article
            logger.info(f"Waiting for page to load for {url.human_repr()}")
            article_div = page.locator("div.layout-article")
            article_div.wait_for(timeout=90000)

            # Get the unix time when the article was created
            datetime_str = article_div.locator(
                "div.article__info-date a"
            ).first.inner_text()

            assert isinstance(datetime_str, str)

            datetime_obj = arrow.get(
                datetime_str, "HH:mm DD.MM.YYYY", tzinfo=pytz.timezone("Europe/Moscow")
            )
            metadata = DefaultArticleMetadata(datetime=datetime_obj, url=url)

            # Get the different pieces of text in the article
            title = article_div.locator(".article__title").inner_text()
            lead = article_div.locator(".article__second-title")
            if lead.count() > 0:
                lead = lead.inner_text()
            else:
                lead = ""
            body = article_div.locator(
                ".article__body .article__block .article__text"
            ).all_inner_texts()
            body = re.sub("\n", " ", re.sub("\n\n", "\n", " ".join(body)))

            return DefaultArticleText(
                title=title, lead=lead, body=body, metadata=metadata
            )


@dataclass(frozen=True)
class RiaArticleUrlScraper:
    """Scrapes the HTML of a Tass webpage for links to articles."""

    href_pattern = re.compile(r".*https://ria\.ru\/\d+\/[^ ]+$")

    def __call__(self, url: URL) -> Iterable[URL]:
        assert "ria.ru" in url.human_repr()
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page()
            logger.info(f"Loading page for '{url.human_repr()}'")
            page.goto(url.human_repr(), timeout=90000)

            # This forces the code to wait for the
            # page to actually load, since tass needs
            # to run js script which loads the ACTUAL content.
            content = page.locator(".content#content")
            content.wait_for(timeout=90000)
            results = set()
            a_tags = content.locator("a")
            for tag_index in range(a_tags.count()):
                href = a_tags.nth(tag_index).get_attribute("href")
                if href is not None and self.href_pattern.match(href) is not None:
                    results.add(URL(href))
            return results


cached_ria_article_retriever = lru_cache()(RiaArticleRetriever())
cached_ria_article_url_scraper = lru_cache()(RiaArticleUrlScraper())
