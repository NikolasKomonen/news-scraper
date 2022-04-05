from abc import abstractmethod
from typing import Collection, Protocol, runtime_checkable

from news_scraper.article.protocols import ArticleText
from news_scraper.word.protocols import TextWithAliases


class WordCounter(Protocol):
    def __call__(self, text: str, words: Collection) -> int:
        """"""


@runtime_checkable
class ArticleWithWordCount(Protocol):
    @property
    @abstractmethod
    def word_with_aliases(self) -> TextWithAliases:
        """"""

    @property
    def title(self) -> int:
        """"""

    @property
    def lead(self) -> int:
        """"""

    @property
    def body(self) -> int:
        """"""

    @property
    def article_text(self) -> ArticleText:
        """"""


class ArticleWordCountToDict(Protocol):
    @property
    @abstractmethod
    def __call__(self, word_counts: ArticleWithWordCount) -> dict:
        """"""


class ArticleWordCounter(Protocol):
    @abstractmethod
    def __call__(
        self, text: ArticleText, words: TextWithAliases
    ) -> ArticleWithWordCount:
        """"""


class ArticleWithWordCountsDictFactory:
    """Converts the given object to a dict of a certain format."""

    @abstractmethod
    def __call__(self, data: ArticleWithWordCount) -> dict:
        """"""
