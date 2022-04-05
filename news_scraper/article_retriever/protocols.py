from abc import abstractmethod
from dataclasses import dataclass
from typing import Iterable, Optional, Protocol
from yarl import URL

from news_scraper.article.protocols import ArticleText


class ArticleRetriever(Protocol):
    @abstractmethod
    def __call__(url: URL) -> Optional[ArticleText]:
        """"""


class ArticleRetrieverResolver(Protocol):
    @abstractmethod
    def __call__(self, url: URL) -> ArticleRetriever:
        """"""


class ArticleUrlScraper(Protocol):
    """Given a URL to a web page, scrape the
    page for links to articles."""

    @abstractmethod
    def __call__(self, url: URL) -> Iterable[URL]:
        """"""


class ArticleUrlScraperResolver(Protocol):
    """Given a URL to a web page, scrape the
    page for links to articles."""

    @abstractmethod
    def __call__(self, url: URL) -> ArticleUrlScraper:
        """"""
