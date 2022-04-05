from dataclasses import dataclass, field
import datetime

import pandas as pd
from yarl import URL
from news_scraper.article_retriever.wayback import WAYBACK_ARCHIVE_PATTERN

from news_scraper.word_counter.protocols import (
    ArticleWithWordCount,
    ArticleWordCountToDict,
)


@dataclass(frozen=True)
class DefaultArticleWordCountToDict:
    """Converts the given object to a dict of a certain format."""

    def __call__(self, word_counts: ArticleWithWordCount) -> dict:
        root_word = word_counts.word_with_aliases.root
        aliases = word_counts.word_with_aliases.aliases
        text = word_counts.article_text
        metadata = text.metadata
        hostname = metadata.url.host.lower()
        date = metadata.datetime.isoformat(sep=" ")
        return {
            "Website": hostname,
            "Date": date,
            "Article": metadata.url.human_repr(),
            "Word": root_word,
            "Word Aliases": ", ".join(aliases),
            "Title": word_counts.title,
            "Lead": word_counts.lead,
            "Body": word_counts.body,
        }


@dataclass(frozen=True)
class WaybackArticleWordCountToDict(DefaultArticleWordCountToDict):
    """Handles edge case for wayback links."""

    def __call__(self, word_counts: ArticleWithWordCount) -> dict:
        res = super().__call__(word_counts)
        match = WAYBACK_ARCHIVE_PATTERN.match(
            word_counts.article_text.metadata.url.human_repr()
        )
        if match is not None:
            res["Website"] = URL(match.group("URL")).host
        return res


@dataclass(frozen=True)
class ArticleWordCountsToCsv:

    word_count_serializer: ArticleWordCountToDict = field(
        default_factory=WaybackArticleWordCountToDict
    )

    def __call__(
        self, word_counts: set[ArticleWithWordCount], filename: str = "word_counts.csv"
    ) -> None:
        """"""
        rows = (self.word_count_serializer(wc) for wc in word_counts)
        df = pd.DataFrame(rows)
        df.to_csv(filename, index=False)
