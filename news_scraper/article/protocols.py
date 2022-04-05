from abc import abstractmethod
from typing import Protocol, runtime_checkable

from yarl import URL
from datetime import datetime


@runtime_checkable
class ArticleMetadata(Protocol):
    @property
    @abstractmethod
    def datetime(self) -> datetime:
        """"""

    @property
    @abstractmethod
    def url(self) -> URL:
        """"""


@runtime_checkable
class ArticleText(Protocol):
    @property
    @abstractmethod
    def title(self) -> str:
        """"""

    @property
    @abstractmethod
    def lead(self) -> str:
        """"""

    @property
    @abstractmethod
    def body(self) -> str:
        """"""

    @property
    @abstractmethod
    def metadata(self) -> ArticleMetadata:
        """"""
