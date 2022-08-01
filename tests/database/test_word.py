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

    def test_adding_word_multiple_times_doesnt_modify(
        self, root_word_table: RootWordTable
    ):
        root_word_table.add_word("Apple")
        root_word_table.add_word("Orange")

        assert root_word_table.get_id_of_word("Apple") == 1
        assert root_word_table.get_id_of_word("Orange") == 2

        root_word_table.add_word("Apple")
        assert root_word_table.get_id_of_word("Apple") == 1


class TestAliasOfRootWordTable:
    @pytest.fixture
    def alias_of_root_word_table(
        self, mock_connection: Connection
    ) -> AliasOfRootWordTable:
        return AliasOfRootWordTable(mock_connection)

    @pytest.fixture
    def alias_words(self) -> Sequence[str]:
        """The words of the alias"""
        return ["ABC", "DEF", "GHI"]

    @pytest.fixture
    def root_word_and_id(self, root_word_table: RootWordTable) -> tuple[str, int]:
        root_word = "MyRootWord"
        root_word_table.add_word(root_word)
        root_word_id = root_word_table.get_id_of_word(root_word)
        return root_word, root_word_id
    
    @pytest.fixture
    def other_root_word_and_id(self, root_word_table: RootWordTable) -> tuple[str, int]:
        root_word = "MyOtherRootWord"
        root_word_table.add_word(root_word)
        root_word_id = root_word_table.get_id_of_word(root_word)
        return root_word, root_word_id

    def test_add_aliases_and_get_them_from_matching_root(
        self,
        alias_of_root_word_table: AliasOfRootWordTable,
        alias_words: Sequence[str],
        root_word_and_id: tuple[str, int],
    ):
        _, root_word_id = root_word_and_id

        # Add aliases to root word
        for alias_word in alias_words:
            alias_of_root_word_table.add_word(root_word_id, alias_word)

        res = alias_of_root_word_table.get_aliases_for_root(root_word_id)
        assert res == alias_words

    def test_add_aliases_and_get_them_from_matching_root(
        self,
        alias_of_root_word_table: AliasOfRootWordTable,
        alias_words: Sequence[str],
        root_word_and_id: tuple[str, int],
        other_root_word_and_id: tuple[str, int],
    ):
        _, root_word_id = root_word_and_id
        _, other_word_id = other_root_word_and_id

        # Add aliases to root word
        for alias_word in alias_words:
            alias_of_root_word_table.add_word(root_word_id, alias_word)
        
        # Add aliases to root word
        for alias_word in alias_words:
            alias_of_root_word_table.add_word(other_word_id, alias_word)

        res = alias_of_root_word_table.get_aliases_for_root(root_word_id)
        assert res == alias_words

        res = alias_of_root_word_table.get_aliases_for_root(other_word_id)
        assert res == alias_words