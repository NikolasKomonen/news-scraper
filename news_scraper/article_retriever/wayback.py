from dataclasses import dataclass
import dataclasses
from functools import lru_cache
import re
from typing import Iterable, Protocol

from yarl import URL
from news_scraper.article.article import DefaultArticleMetadata

from news_scraper.article.protocols import ArticleText
from news_scraper.article_retriever.lenta import LentaArticleUrlScraper
from news_scraper.article_retriever.novayagazeta import NovayagazetaArticleUrlScraper
from news_scraper.article_retriever.ria import RiaArticleUrlScraper
from news_scraper.article_retriever.tass import (
    TassArticleUrlScraper,
    cached_tass_article_retriever,
)

from news_scraper.article_retriever.moscow_times import (
    MoscowTimesArticleUrlScraper,
)

WAYBACK_ARCHIVE_PATTERN = re.compile(
    r"https?\:\/\/web\.archive\.org\/web\/\d+\/(?P<URL>.*)"
)


@dataclass(frozen=True)
class WayBackMachineArticleRetriever:
    def __call__(self, url: URL) -> ArticleText:
        """"""
        match = WAYBACK_ARCHIVE_PATTERN.match(url.human_repr())
        if not match:
            raise Exception("URL given was not from the wayback machine")
        actual_url = URL(match.group("URL"))
        if "tass" in actual_url.human_repr():
            
            url_to_scrape = url
            # url_to_scrape = actual_url
            
            article_text = cached_tass_article_retriever(url_to_scrape)

            # if article_text.metadata.url != url:
            #     metadata = DefaultArticleMetadata(datetime=article_text.metadata.datetime, url=url)
            #     article_text = dataclasses.replace(article_text, metadata=metadata)
            
            return article_text
        raise Exception(
            f"Article retriever for '{actual_url.human_repr()}' in Wayback Machine not implemented."
        )


@dataclass(frozen=True)
class WayBackMachineArticleUrlScraper:

    tass_scraper = TassArticleUrlScraper()
    moscow_times_scraper = MoscowTimesArticleUrlScraper()
    novayagazeta_scraper = NovayagazetaArticleUrlScraper()
    ria_scraper = RiaArticleUrlScraper()
    lenta_scraper = LentaArticleUrlScraper()

    def __call__(self, url: URL) -> Iterable[URL]:
        """"""
        match = WAYBACK_ARCHIVE_PATTERN.match(url.human_repr())
        article_url = URL(match.group("URL"))
        if not match:
            raise Exception("URL given was not from the wayback machine")
        if "tass" in url.path:
            return self.tass_scraper(article_url)
        if "moscowtimes" in url.path:
            return self.moscow_times_scraper(url)
        if "ria" in url.path:
            return self.ria_scraper(url)
        if "novayagazeta" in url.path:
            return self.novayagazeta_scraper(url)
        if "lenta" in url.path:
            return self.lenta_scraper(url)
        with open("unscraped_pages.csv", "a+") as f:
            f.write(f"{url.human_repr()}\n")


cached_wayback_machine_article_retriever = lru_cache()(WayBackMachineArticleRetriever())
