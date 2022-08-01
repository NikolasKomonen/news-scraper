from unittest.mock import MagicMock, call, create_autospec
import pytest

from news_scraper.database.sqlite.client import AliasOfRootWordTable, RootWordTable
from news_scraper.word.protocols import TextWithAliases
from news_scraper.word.save import SqlSaveTextWithAliases
from news_scraper.word.word import DefaultTextWithAliases


class TestSqlSaveTextWithAliases:
    """"""

    @pytest.fixture
    def root_word_table(self) -> MagicMock:
        return create_autospec(RootWordTable, instance=True)

    @pytest.fixture
    def alias_of_root_word_table(self) -> MagicMock:
        return create_autospec(AliasOfRootWordTable, instance=True)

    @pytest.fixture
    def text_with_aliases_instance(self) -> DefaultTextWithAliases:
        return DefaultTextWithAliases(root="MyRoot", aliases=["a1", "a2"])

    def test_sql_save_text_with_aliases(
        self,
        root_word_table: MagicMock,
        alias_of_root_word_table: MagicMock,
        text_with_aliases_instance: TextWithAliases,
    ) -> None:
        root_word_id = 1
        root_word_table.get_id_of_word.return_value = root_word_id

        SqlSaveTextWithAliases(root_word_table, alias_of_root_word_table)(
            text_with_aliases_instance
        )
        assert root_word_table.add_word.call_args_list == [
            call(text_with_aliases_instance.root)
        ]
        assert root_word_table.get_id_of_word.call_args_list == [
            call(text_with_aliases_instance.root)
        ]
        assert alias_of_root_word_table.add_word.call_args_list == [
            call(root_word_id, alias) for alias in text_with_aliases_instance.aliases
        ]
