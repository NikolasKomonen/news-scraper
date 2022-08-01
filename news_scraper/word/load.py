from typing import Protocol, Sequence
from news_scraper.database.sqlite.client import AliasOfRootWordTable, RootWordTable

from news_scraper.word.protocols import TextWithAliases
from news_scraper.word.word import DefaultTextWithAliases


class LoadTextWithAliases(Protocol):
    root_word_table: RootWordTable
    alias_of_root_word_table: AliasOfRootWordTable

    def __call__(self, root_word: str) -> Sequence[TextWithAliases]:
        root_id = self.root_word_table.get_id_of_word(root_word)
        aliases = set(self.alias_of_root_word_table.get_aliases_for_root(root_id))
        return DefaultTextWithAliases(
            root_word, aliases
        )
