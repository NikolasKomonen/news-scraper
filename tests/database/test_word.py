from pathlib import Path
from sqlite3 import Connection, connect
from typing import Sequence
import pytest

from news_scraper.database.sqlite.client import AliasOfRootWordTable, RootWordTable


@pytest.fixture
def mock_connection(tmp_path: Path) -> Connection:
    db_location = tmp_path / "test.db"
    db_location.parent.mkdir(parents=True, exist_ok=True)
    return connect(str(db_location))


@pytest.fixture
def root_word_table(mock_connection: Connection) -> RootWordTable:
    return RootWordTable(mock_connection)


class TestRootWordTable:
    @pytest.fixture
    def root_word_table_words(self, root_word_table: RootWordTable) -> Sequence[str]:
        words = ["root1", "root2"]
        for word in words:
            root_word_table.add_word(word)
        return words

    def test_add_words_then_get_their_ids(
        self, root_word_table: RootWordTable, root_word_table_words: Sequence[str]
    ) -> None:
        id_counter = 1  # sqlite IDs start at 1
        for word in root_word_table_words:
            word_id = root_word_table.get_id_of_word(word)
            assert word_id == id_counter, f"Failure with '{word}' of id '{word_id}'"
            id_counter += 1


class TestAliasOfRootWordTable:
    @pytest.fixture
    def alias_of_root_word_table(self, mock_connection: Connection) -> AliasOfRootWordTable:
        return AliasOfRootWordTable(mock_connection)

    @pytest.fixture
    def alias_words(self) -> Sequence[str]:
        """The words of the alias"""
        return ["ABC", "DEF", "GHI"]

    def test_add_aliases_and_get_them_from_matching_root(
        self,
        root_word_table: RootWordTable,
        alias_of_root_word_table: AliasOfRootWordTable,
        alias_words: Sequence[str]
    ):
        # Create root word
        root_word = "MyRootWord"
        root_word_table.add_word(root_word)
        root_word_id = root_word_table.get_id_of_word(root_word)

        # Add aliases to root word
        for alias_word in alias_words:
            alias_of_root_word_table.add_word(root_word_id, alias_word)

        res = alias_of_root_word_table.get_aliases_for_root(root_word_id)
        assert res == alias_words
