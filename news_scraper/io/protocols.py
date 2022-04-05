from abc import abstractmethod
from typing import Iterable, Protocol

from yarl import URL

from news_scraper.word.protocols import TextWithAliases


class UrlLoader(Protocol):
    @abstractmethod
    def __call__(self, filename: str) -> Iterable[URL]:
        """"""


class WordWithAliasesLoader(Protocol):
    @abstractmethod
    def __call__(self, filename: str) -> Iterable[TextWithAliases]:
        """"""
