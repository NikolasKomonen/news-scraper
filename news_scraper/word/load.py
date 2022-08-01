from dataclasses import dataclass
from sqlite3 import Connection
from typing import Protocol, Sequence
from news_scraper.database.sqlite.client import AliasOfRootWordTable, RootWordTable

from news_scraper.word.protocols import TextWithAliases
from news_scraper.word.word import DefaultTextWithAliases


@dataclass
class GetTextWithAliases:
    root_word_table: RootWordTable
    alias_of_root_word_table: AliasOfRootWordTable

    def __call__(self, root_word: str) -> TextWithAliases:
        root_id = self.root_word_table.get_id_of_word(root_word)
        aliases = set(self.alias_of_root_word_table.get_aliases_for_root(root_id))
        return DefaultTextWithAliases(
            root_word, aliases
        )

def load_all_text_with_aliases(conn: Connection) -> Sequence[TextWithAliases]:
    root_word_table = RootWordTable(conn)
    alias_of_root_word_table = AliasOfRootWordTable(conn)

    getter = GetTextWithAliases(root_word_table, alias_of_root_word_table)

    return [getter(root_word) for _, root_word in root_word_table.get_all_words()]
        