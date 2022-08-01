from abc import abstractmethod
from typing import Protocol


class TextWithAliases(Protocol):
    @property
    def root(self) -> str:
        """"""

    @property
    def aliases(self) -> set[str]:
        """"""


class SaveTextWithAliases(Protocol):
    @abstractmethod
    def __call__(self, twa: TextWithAliases) -> None:
        """"""
