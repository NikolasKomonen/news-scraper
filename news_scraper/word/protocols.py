from typing import Protocol


class TextWithAliases(Protocol):
    @property
    def root(self) -> str:
        """"""

    @property
    def aliases(self) -> set[str]:
        """"""
