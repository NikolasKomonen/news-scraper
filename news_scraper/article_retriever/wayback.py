from dataclasses import dataclass
import dataclasses
from functools import lru_cache
import logging
import re
from typing import Iterable, Optional, Protocol

from yarl import URL
from news_scraper.article.article import DefaultArticleMetadata

from news_scraper.article.protocols import ArticleText
from news_scraper.article_retriever.lenta import LentaArticleUrlScraper
from news_scraper.article_retriever.novayagazeta import NovayagazetaArticleUrlScraper
from news_scraper.article_retriever.protocols import ArticleRetriever
from news_scraper.article_retriever.ria import RiaArticleRetriever, RiaArticleUrlScraper
from news_scraper.article_retriever.tass import (
    TassArticleRetriever,
    TassArticleUrlScraper,
    cached_tass_article_retriever,
)

from news_scraper.article_retriever.moscow_times import (
    MoscowTimesArticleRetriever,
    MoscowTimesArticleUrlScraper,
)

logger = logging.getLogger()

WAYBACK_ARCHIVE_PATTERN = re.compile(
    r"https?\:\/\/web\.archive\.org\/web\/[\da-z\_]+\/(?P<URL>.*)"
)

def determine_actual_article_retriever(wayback_url: URL) -> Optional[ArticleRetriever]:
    match = WAYBACK_ARCHIVE_PATTERN.match(wayback_url.human_repr())
    if not match:
        return None
    url_text = match.group("URL")
    if "tass.com" in url_text:
        return TassArticleRetriever()
    if "tass.ru" in url_text:
        return TassArticleRetriever()
    if "themoscowtimes.com" in url_text:
        return MoscowTimesArticleRetriever()
    if "ria.ru" in url_text:
        return RiaArticleRetriever()
    return None

@dataclass(frozen=True)
class WayBackMachineArticleRetriever:
    def __call__(self, url: URL) -> ArticleText:
        """"""
        actual_retriever = determine_actual_article_retriever(url)
        assert actual_retriever is not None, "URL given was not from the wayback machine"
        
        original_article_url = None
        match = WAYBACK_ARCHIVE_PATTERN.match(url.human_repr())
        if not match:
            raise Exception(f"URL is not a wayback article url: '{url.human_repr()}'")
        original_article_url = URL(match.group("URL"))
        
        
        try:
            article_text = actual_retriever(url)
        except Exception as e:
            if original_article_url is not None:
                logger.info(f"Using backup url '{original_article_url.human_repr()}'")
                article_text = actual_retriever(original_article_url)
            else:
                raise e
        assert isinstance(article_text, ArticleText), f"No articel retriever can be resolved for {url.human_repr()}"

        if article_text.metadata.url != original_article_url:
            metadata = DefaultArticleMetadata(datetime=article_text.metadata.datetime, url=original_article_url)
            article_text = dataclasses.replace(article_text, metadata=metadata)
        
            
        return article_text
        


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
