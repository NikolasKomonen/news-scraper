from dataclasses import dataclass
from news_scraper.database.sqlite.client import AliasOfRootWordTable, RootWordTable

from news_scraper.word.protocols import SaveTextWithAliases, TextWithAliases


@dataclass(frozen=True)
class SqlSaveTextWithAliases(SaveTextWithAliases):
    root_word_table: RootWordTable
    alias_of_root_word_table: AliasOfRootWordTable

    def __call__(self, twa: TextWithAliases) -> None:
        self.root_word_table.add_word(twa.root)
        root_id: int = self.root_word_table.get_id_of_word(twa.root)

        for alias in twa.aliases:
            self.alias_of_root_word_table.add_word(root_id, alias)

