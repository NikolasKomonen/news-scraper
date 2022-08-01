from typing import Collection
import pytest
from news_scraper.word.protocols import TextWithAliases
from news_scraper.word.word import NormalizedTextWithAliases

from news_scraper.word_counter.fuzzy_word_counter import FuzzyWordVariantCounter


class TestFuzzyWordVariantCounter:
    @pytest.fixture(scope="class")
    def fuzzy_word_counter_default(self) -> FuzzyWordVariantCounter:
        return FuzzyWordVariantCounter()

    @pytest.mark.parametrize(
        ["words", "text", "expected"],
        [
            pytest.param(
                NormalizedTextWithAliases(""), "apple", 0, id="Empty string for match."
            ),
            pytest.param(
                NormalizedTextWithAliases("tree"), "apple", 0, id="No matches."
            ),
            pytest.param(
                NormalizedTextWithAliases("apple"), "apple", 1, id="Exact same word"
            ),
            pytest.param(
                NormalizedTextWithAliases("apple"), "mapple", 1, id="One character off"
            ),
            pytest.param(
                NormalizedTextWithAliases("apple", {"mapple", "apples"}),
                "mapple",
                1,
                id="Counts once even if multiple matches",
            ),
            pytest.param(
                NormalizedTextWithAliases("putin"),
                "pyutin and putin and potin",
                3,
                id="Matches similar names",
            ),
        ],
    )
    def test_default_ratio_matches(
        self,
        words: TextWithAliases,
        text: str,
        expected: int,
        fuzzy_word_counter_default: FuzzyWordVariantCounter,
    ):
        assert fuzzy_word_counter_default(text, words) == expected

    @pytest.fixture(scope="class")
    def fuzzy_word_counter_exact(self) -> FuzzyWordVariantCounter:
        return FuzzyWordVariantCounter(match_ratio=100)

    @pytest.mark.parametrize(
        ["words", "text", "expected"],
        [
            pytest.param(
                NormalizedTextWithAliases("apple"), "apple", 1, id="Exact same word"
            ),
            pytest.param(
                NormalizedTextWithAliases("apple"), "mapple", 0, id="Exact same word"
            ),
        ],
    )
    def test_exact_matches(
        self,
        words: Collection[str],
        text: str,
        expected: int,
        fuzzy_word_counter_exact: FuzzyWordVariantCounter,
    ):
        assert fuzzy_word_counter_exact(text, words) == expected
