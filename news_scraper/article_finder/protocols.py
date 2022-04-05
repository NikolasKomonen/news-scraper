from typing import Protocol

from yarl import URL


class ArticleFinder(Protocol):
    def __call__(self, url: URL) -> URL:
        """"""
