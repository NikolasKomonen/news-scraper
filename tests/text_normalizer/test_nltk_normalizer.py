from nltk.corpus import stopwords
import re

import pytest

from news_scraper.text_normalizer.nltk_normalizer import NltkStringNormalizer


class TestNltkStringNormalizer:
    @pytest.fixture
    def nltk_string_normalizer(self) -> NltkStringNormalizer:
        return NltkStringNormalizer()

    @pytest.mark.parametrize(
        ["input", "output"],
        [
            pytest.param("Nikolas", "nikolas", id="Word gets lowercased"),
            pytest.param(
                "Nikolas ALWAYS Cool",
                "nikolas always cool",
                id="Multiple words gets lowercased",
            ),
            pytest.param(
                "John W. Deez-Nuts (formerly Ben)!",
                "john w deeznuts formerly ben",
                id="Specials chars removed",
            ),
            pytest.param(
                "i nikolas can have his or hers a why komonen",
                "nikolas komonen",
                id="stopwords are removed",
            ),
            pytest.param(
                "Covid-19",
                "covid19",
                id="Numbers in name",
            ),
            pytest.param(
                "LDPR’s",
                "ldprs",
                id="Removes non-ascii character",
            ),
        ],
    )
    def test_main(
        self, input: str, output: str, nltk_string_normalizer: NltkStringNormalizer
    ):
        assert nltk_string_normalizer(input) == output
