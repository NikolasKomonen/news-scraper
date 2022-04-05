from typing import Protocol


class StringNormalizer(Protocol):
    def __call__(self, text: str) -> str:
        """"""
