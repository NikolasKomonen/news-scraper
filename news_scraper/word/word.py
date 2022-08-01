from dataclasses import dataclass, field
from news_scraper.text_normalizer.nltk_normalizer import cached_nltk_string_normalizer

from news_scraper.text_normalizer.protocols import StringNormalizer
from news_scraper.word.protocols import TextWithAliases


@dataclass(frozen=True)
class DefaultTextWithAliases:
    root: str
    aliases: set[str]


@dataclass(frozen=True)
class NormalizedTextWithAliases:
    _root: str
    _aliases: set[str] = field(default_factory=frozenset)

    text_normalizer: StringNormalizer = cached_nltk_string_normalizer

    @property
    def root(self) -> str:
        return self.text_normalizer(self._root)

    @property
    def aliases(self) -> set[str]:
        return set(map(self.text_normalizer, self._aliases))
