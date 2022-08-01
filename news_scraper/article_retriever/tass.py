from dataclasses import dataclass
from functools import lru_cache
import logging
import re
from typing import Iterable
from playwright.sync_api import sync_playwright
from yarl import URL
from news_scraper.article.article import DefaultArticleMetadata, DefaultArticleText

from news_scraper.article.protocols import ArticleText
from datetime import datetime
from playwright.sync_api._generated import Locator

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

URLS_TO_SKIP = re.compile(
    r".+\/((sport)|(business\-officials)|(top\-officials)|(ads)|(armiya-i-opk))\/.+"
)

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
            div = page.locator(".news, .article, .interview, .opinion")

            div.wait_for(timeout=TIMEOUT_MILLISECONDS)
            class_attribute = div.get_attribute("class")
            if class_attribute == "article":
                return self._get_text_from_article(article_div=div, url=url)
            if class_attribute == "news":
                return self._get_text_from_news(news_div=div, url=url)
            if class_attribute == "interview":
                return self._get_text_from_interview(interview_div=div, url=url)
            if class_attribute == "opinion":
                return self._get_text_from_opinion(opinion_div=div, url=url)
            raise Exception("Cannot find a defined element for this page.")

    def _get_text_from_article(self, article_div: Locator, url: URL) -> ArticleText:
        # Get the unix time when the article was created
        datetime_int = int(
            article_div.locator("dateformat").first.get_attribute("time")
        )
        assert isinstance(datetime_int, int)
        datetime_obj = datetime.fromtimestamp(datetime_int)
        metadata = DefaultArticleMetadata(datetime=datetime_obj, url=url)

        # Get the different pieces of text in the article
        title = article_div.locator(".article__title-wrap").all_inner_texts()
        title = " ".join(title)
        lead = article_div.locator(".article-lead")
        lead = "" if lead.count() == 0 else lead.inner_text()
        body = article_div.locator(".text-content .text-block").all_inner_texts()
        body = re.sub("\n", " ", re.sub("\n\n", "\n", " ".join(body)))

        return DefaultArticleText(title=title, lead=lead, body=body, metadata=metadata)

    def _get_text_from_news(self, news_div: Locator, url: URL) -> ArticleText:
        # Get the unix time when the article was created
        datetime_int = int(news_div.locator("dateformat").first.get_attribute("time"))
        assert isinstance(datetime_int, int)
        datetime_obj = datetime.fromtimestamp(datetime_int)
        metadata = DefaultArticleMetadata(datetime=datetime_obj, url=url)

        # Get the different pieces of text in the article
        title = news_div.locator(".news-header__title").inner_text()
        lead = news_div.locator(".news-header__lead").inner_text()
        body = news_div.locator(".text-content .text-block").all_inner_texts()
        body = re.sub("\n", " ", re.sub("\n\n", "\n", " ".join(body)))

        return DefaultArticleText(title=title, lead=lead, body=body, metadata=metadata)

    def _get_text_from_interview(self, interview_div: Locator, url: URL) -> ArticleText:
        # Get the unix time when the article was created
        datetime_int = int(
            interview_div.locator("dateformat").first.get_attribute("time")
        )
        assert isinstance(datetime_int, int)
        datetime_obj = datetime.fromtimestamp(datetime_int)
        metadata = DefaultArticleMetadata(datetime=datetime_obj, url=url)

        # Get the different pieces of text in the article
        title = interview_div.locator(
            ".interview-header__title-wrapper"
        ).all_inner_texts()
        title = " ".join(title)
        lead = ""
        body = interview_div.locator(".text-content .text-block").all_inner_texts()
        body = re.sub("\n", " ", re.sub("\n\n", "\n", " ".join(body)))

        return DefaultArticleText(title=title, lead=lead, body=body, metadata=metadata)

    def _get_text_from_opinion(self, opinion_div: Locator, url: URL) -> ArticleText:
        # Get the unix time when the article was created
        datetime_int = int(
            opinion_div.locator("dateformat").first.get_attribute("time")
        )
        assert isinstance(datetime_int, int)
        datetime_obj = datetime.fromtimestamp(datetime_int)
        metadata = DefaultArticleMetadata(datetime=datetime_obj, url=url)

        # Get the different pieces of text in the article
        title = opinion_div.locator(".opinion-header__title-wrapper").all_inner_texts()
        title = " ".join(title)
        lead = opinion_div.locator(".opinion-header__lead").inner_text()
        body = opinion_div.locator(".text-content .text-block").all_inner_texts()
        body = re.sub("\n", " ", re.sub("\n\n", "\n", " ".join(body)))

        return DefaultArticleText(title=title, lead=lead, body=body, metadata=metadata)


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
