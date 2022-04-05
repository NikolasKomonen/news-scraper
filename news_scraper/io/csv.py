from dataclasses import dataclass
import logging
from typing import Iterable
import pandas
from yarl import URL

from news_scraper.word.protocols import TextWithAliases
from news_scraper.word.word import NormalizedTextWithAliases


@dataclass(frozen=True)
class CsvUrlReader:
    """
    This loads all URLs from a CSV file.
    for the given column index.

    :param column: The column that has the URLs, starts
                   at 0.
    :param has_header: If the first row has header names,
                       if True it skips the first row.
    """

    logger = logging.getLogger("csv-loader")

    def __call__(
        self, filename: str, column: int = 0, has_header: bool = False
    ) -> Iterable[URL]:
        header = 0 if has_header else None
        csv = pandas.read_csv(filename, header=header)
        for _, row in csv.iterrows():
            if pandas.isna(row[column]):
                self.logger.warning(
                    f"Row did not have a url at column '{column}'.", row
                )
                continue
            yield URL(row[column])


@dataclass(frozen=True)
class CsvUrlWriter:
    """
    This loads all URLs from a CSV file.
    for the given column index.

    :param column: The column that has the URLs, starts
                   at 0.
    :param has_header: If the first row has header names,
                       if True it skips the first row.
    """

    logger = logging.getLogger("csv-loader")

    def __call__(self, filename: str, rows: list[dict]) -> Iterable[URL]:
        df = pandas.DataFrame(rows)
        df.to_csv(filename, index=False)


@dataclass(frozen=True)
class CsvWordWithAliasesLoader:
    def __call__(self, filename: str) -> Iterable[TextWithAliases]:
        out = pandas.read_csv(filename, header=None)
        for _, row in out.iterrows():
            row = filter(lambda x: not pandas.isna(x), row.__iter__())
            yield NormalizedTextWithAliases(next(row), frozenset(row))
