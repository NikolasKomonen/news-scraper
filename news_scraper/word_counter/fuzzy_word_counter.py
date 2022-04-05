from dataclasses import dataclass

from news_scraper.word.protocols import TextWithAliases

from thefuzz import fuzz


@dataclass(frozen=True)
class FuzzyWordVariantCounter:

    match_ratio: int = 80

    def __call__(self, text: str, words: TextWithAliases) -> int:
        text = text.split()
        search_words = {words.root}.union(words.aliases)
        counter = (
            1
            if any(
                (fuzz.ratio(search_word, word) >= self.match_ratio)
                for search_word in search_words
            )
            else 0
            for word in text
        )
        return sum(counter)
