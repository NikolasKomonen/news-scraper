from abc import abstractmethod
from datetime import datetime
from enum import Enum
from pathlib import Path
from sqlite3 import Connection, Cursor, connect
from textwrap import dedent
from typing import Iterable, Optional, Tuple
import arrow
import pytz

from yarl import URL
from news_scraper.article.article import DefaultArticleMetadata, DefaultArticleText

from news_scraper.article.protocols import ArticleMetadata, ArticleText
from news_scraper.article_retriever.wayback import WAYBACK_ARCHIVE_PATTERN

SQL_NULL = "NULL"


class SqliteConnection:
    def __init__(self) -> None:
        self.client = connect("")

    def db_dir_location(self, context_dir: Optional[Path] = None) -> Path:
        if context_dir is None:
            context_dir = Path.cwd()
        return context_dir / "news_scraper_db"


class NewsScraperSqliteConnection:
    @classmethod
    def connection(cls) -> Connection:
        db_location = Path("news_scraper_db/news_scraper.db")
        db_location.parent.mkdir(parents=True, exist_ok=True)
        return connect(str(db_location))


class SqlTable:
    def __init__(self, connection: Connection) -> None:
        self.connection = connection
        self.cursor = connection.cursor()

    @abstractmethod
    def create_table(self) -> None:
        """"""

    @property
    @abstractmethod
    def name(self) -> str:
        """Name of table"""

    def query(self, query: str) -> Cursor:
        return self.cursor.execute(query)

    def transaction(self, transaction: str, parameters: Optional[Iterable]=None):
        if parameters is None:
            parameters = []
        self.cursor.execute(transaction, parameters)
        self.connection.commit()

    def transactionMany(self, transaction: str, inputs: Iterable[Iterable]):
        self.cursor.executemany(transaction, inputs)
        self.connection.commit()

    def isoformat(self, dt: datetime) -> str:
        if dt.tzinfo is not None:
            dt = dt.astimezone(pytz.UTC)
            dt = dt.replace(tzinfo=None)
        dt = dt.replace(microsecond=0)
        return dt.isoformat(sep=" ")

    def url_to_str(self, url: URL) -> str:
        return url.human_repr()


class NewsWebsiteEnum(Enum):
    THE_MOSCOW_TIMES = "themoscowtimes.com"
    NOVAYAGAZETA = "novayagazeta.ru"
    RIA = "ria.ru"
    LENTA = "lenta.ru"
    TASS_RU = "tass.ru"
    TASS = "tass.com"

    @classmethod
    def from_url(cls, url: URL):
        match = WAYBACK_ARCHIVE_PATTERN.match(url.human_repr())
        if match:
            return cls(URL(match.group("URL")).host)
        return cls(url.host)


class NewsWebsite(SqlTable):
    def __init__(self, connection: Connection) -> None:
        """"""
        super().__init__(connection)
        self.create_table()
        self.create_initial_data()
    
    @classmethod
    @property
    def foreign_id(cls) -> str:
        news_site = NewsWebsite.name
        return f"{news_site}_id"

    @classmethod
    @property
    def name(cls) -> str:
        return "news_site"

    @classmethod
    @property
    def domain(cls) -> str:
        return "domain"

    def create_table(self):
        self.transaction(
            f"""
            CREATE TABLE IF NOT EXISTS "{self.name}" (
                "id"	 INTEGER NOT NULL,
                "{self.domain}" TEXT NOT NULL UNIQUE,
                PRIMARY KEY("id" AUTOINCREMENT)
            );
        """
        )

    def create_initial_data(self) -> None:
        self.transaction(
            f"""
            INSERT INTO {self.name}
            VALUES
                (1, 'themoscowtimes.com'),
                (2, 'novayagazeta.ru'),
                (3, 'ria.ru'),
                (4, 'lenta.ru'),
                (5, 'tass.ru'),
                (6, 'tass.com')
            ON CONFLICT DO NOTHING;
        """
        )

    def get_id(self, news_site: NewsWebsiteEnum) -> int:
        query = self.query(self._get_id(news_site))
        res = next(iter(query), None)
        if res is None:
            raise Exception("Invalid news site.")
        return res[0]

    @classmethod
    def _get_id(cls, news_site: NewsWebsiteEnum) -> str:
        return dedent(
            f"""
            SELECT id
            FROM {cls.name}
            WHERE domain = '{news_site.value}'
        """
        ).strip()

    def get_domain(self, id: int) -> NewsWebsiteEnum:
        query = self.query(self._get_domain(id))
        res = next(iter(query), None)
        if res is None:
            raise Exception("Invalid news site id.")
        return NewsWebsiteEnum(res[0])

    @classmethod
    def _get_domain(cls, id: int) -> str:
        return f"""
            SELECT domain
            FROM {cls.name}
            WHERE id = {id}
        """


class WaybackResult(SqlTable):
    def __init__(self, connection: Connection) -> None:
        """"""
        super().__init__(connection)
        self.create_table()

    @classmethod
    @property
    def name(cls) -> str:
        return "wayback_result"

    @classmethod
    @property
    def requested_url(cls) -> str:
        return "requested_url"

    @classmethod
    @property
    def requested_ts(cls) -> str:
        return "requested_ts"

    @classmethod
    @property
    def actual_url(cls) -> str:
        return "actual_url"

    @classmethod
    @property
    def actual_ts(cls) -> str:
        return "actual_ts"

    def create_table(self) -> None:

        self.transaction(
            f"""
            CREATE TABLE IF NOT EXISTS {self.name} (
                "id"                   INTEGER,
                "{NewsWebsite.foreign_id}"  INTEGER,
                "{self.requested_url}" TEXT NOT NULL,
                "{self.requested_ts}"  TEXT NOT NULL,
                "{self.actual_url}"    TEXT,
                "{self.actual_ts}"	   TEXT,
                PRIMARY KEY("id" AUTOINCREMENT),
                UNIQUE("{self.requested_url}" ,"{self.requested_ts}"),
                FOREIGN KEY("{NewsWebsite.foreign_id}") REFERENCES "{NewsWebsite.name}"("id")
            );
        """
        )

    def insert_row(
        self,
        news_site: NewsWebsiteEnum,
        requested_url: URL,
        requested_ts: datetime,
        actual_url: Optional[URL],
        actual_ts: Optional[datetime],
    ):
        """"""
        query = self._insert_row(
            news_site, requested_url, requested_ts, actual_url, actual_ts
        )
        self.transaction(query)

    def _insert_row(
        self,
        news_site: NewsWebsiteEnum,
        requested_url: URL,
        requested_ts: datetime,
        actual_url: Optional[URL],
        actual_ts: Optional[datetime],
    ) -> str:
        requested_url = self.url_to_str(requested_url)
        request_ts = self.isoformat(requested_ts)
        actual_url = f'"{self.url_to_str(actual_url)}"' if actual_url else SQL_NULL
        actual_ts = f'"{self.isoformat(actual_ts)}"' if actual_ts else SQL_NULL
        site_alias = "site"
        return dedent(
            f"""\
            INSERT OR REPLACE INTO {self.name}
                ({NewsWebsite.foreign_id},
                {self.requested_url},
                {self.requested_ts},
                {self.actual_url},
                {self.actual_ts})
            SELECT
                {site_alias}.id,
                "{requested_url}",
                "{request_ts}",
                {actual_url},
                {actual_ts}
            FROM (
                {NewsWebsite._get_id(news_site)}
                LIMIT 1
            ) AS {site_alias}
        """
        ).strip()

    def request_exists(
        self,
        requested_url: URL,
        requested_ts: datetime,
    ) -> bool:
        """"""
        query = self._request_exists(requested_url, requested_ts)
        res = self.query(query)
        if res is None:
            return False
        val = next(iter(res))
        return val[0] > 0

    def _request_exists(
        self,
        requested_url: URL,
        requested_ts: datetime,
    ) -> bool:
        """"""
        return dedent(
            f"""
            SELECT COUNT(id)
            FROM {self.name}
            WHERE
                {self.requested_url} = "{self.url_to_str(requested_url)}"
                AND
                {self.requested_ts} = "{self.isoformat(requested_ts)}"
                AND
                {self.actual_url} IS NOT NULL
        """
        ).strip()

    def get_urls(self) -> Iterable[URL]:
        rows = self.query(
            f"""
            SELECT {self.actual_url}
            FROM {self.name}
        """
        )
        return [URL(row[0]) for row in rows]

    def _get_id_from_wayback_url(self, url: URL) -> int:
        """
        Gets the id for the row where the given
        url matches the url provided by the wayback
        result, not the url used to request.

        There are scenarios where multiple timestamps
        have the same wayback url. This will return
        the id of the row which has the closest timestamp
        to the time that was requested.
        """
        rows = self.query(
            f"""
            SELECT id, {self.requested_ts}, {self.actual_ts}
            FROM {self.name}
            WHERE {self.actual_url} = "{self.url_to_str(url)}"
        """
        )
        wb_id = 0
        row_iter = iter(rows)
        row = next(row_iter, None)
        if row is None:
            return 0

        # All rows will have the same ts since their
        # URL is the same and WBM's URLs are time specific
        actual_ts = datetime.fromisoformat(row[2])

        wb_id = row[0]
        requested_ts = datetime.fromisoformat(row[1])
        min_delta = abs(requested_ts - actual_ts)

        for row in row_iter:  # This handles multiple rows with the same wb url
            requested_ts = datetime.fromisoformat(row[1])
            new_delta = abs(requested_ts - actual_ts)
            if new_delta < min_delta:
                wb_id = row[0]

        return wb_id

    def _get_days_with_request(self) -> Iterable[Tuple[URL, datetime]]:
        query = dedent(
            f"""
            SELECT {self.requested_url}, SUBSTR(wr.{self.requested_ts}, 0, 11)
            FROM {self.name} as wr
            GROUP BY wr.{self.requested_url}, SUBSTR(wr.{self.requested_ts}, 0, 11)
        """
        ).strip()
        rows = self.query(query)

        return [(URL(row[0], datetime.fromisoformat(row[1]))) for row in rows]

    def get_scraped_urls_for_days(self) -> Iterable[set[URL]]:
        swu = ScrapedWaybackUrl(self.connection)
        for day_with_req in self._get_days_with_request():
            url, day = day_with_req


class ScrapedWaybackUrl(SqlTable):
    def __init__(self, connection: Connection) -> None:
        super().__init__(connection)
        self.create_table()

    @property
    def name(self) -> str:
        return "scraped_wayback_url"

    @property
    def wayback_result_id(self) -> str:
        return f"{WaybackResult.name}_id"

    @property
    def scraped_url(self) -> str:
        return "scraped_url"

    def create_table(self) -> None:
        query = f"""
            CREATE TABLE IF NOT EXISTS "{self.name}" (
                "{self.wayback_result_id}" INTEGER,
                "{self.scraped_url}"	   TEXT NOT NULL,
                FOREIGN KEY("{self.wayback_result_id}") REFERENCES "{WaybackResult.name}"("id"),
                PRIMARY KEY("{self.wayback_result_id}", "{self.scraped_url}")
            );
        """
        self.transaction(query)

    def add_urls(self, wayback_result_id: int, urls: set[URL]):
        self.transactionMany(
            f"""
            REPLACE INTO {self.name}
            VALUES (?, ?)
            """,
            [(wayback_result_id, self.url_to_str(url)) for url in urls],
        )

    def get_urls_with_id(self, wayback_result_id: int) -> Iterable[URL]:
        res = self.query(
            f"""
            SELECT {self.scraped_url}
            FROM {self.name}
            WHERE
                {self.wayback_result_id} = "{wayback_result_id}"
        """
        )
        return [URL(row[0]) for row in res]


    def get_count_for_id(self, wayback_result_id: int) -> int:
        res = self.query(
            f"""
            SELECT COUNT({self.wayback_result_id})
            FROM {self.name}
            WHERE
                {self.wayback_result_id} = "{wayback_result_id}"
        """
        )
        count = next(iter(res))[0]
        return count
    
    def get_unique_urls_from_site(self, news_site: NewsWebsiteEnum) -> Iterable[URL]:
        nws = NewsWebsite(self.connection)
        news_site_id = nws.get_id(news_site)

        query = dedent(f"""
            SELECT DISTINCT {self.scraped_url}
            FROM {self.name}
            JOIN {WaybackResult.name} ON {self.name}.{self.wayback_result_id}={WaybackResult.name}.id
            WHERE
                {WaybackResult.name}.{NewsWebsite.foreign_id}={news_site_id}
        """)
        rows = self.query(query)

        return [URL(row[0]) for row in rows]


class ArticleMetadataTable(SqlTable):
    def __init__(self, connection: Connection) -> None:
        super().__init__(connection)
        self.create_table()

    @classmethod
    @property
    def name(cls) -> str:
        return "article_metadata"

    @classmethod
    @property
    def foreign_id(cls) -> str:
        return f"{cls.name}_id"

    @classmethod
    @property
    def url(cls) -> str:
        return "url"

    @classmethod
    @property
    def datetime(cls) -> str:
        return "datetime"

    def create_table(self) -> None:
        transaction = f"""
            CREATE TABLE IF NOT EXISTS "article_metadata" (
                "id"	          INTEGER,
                "{self.url}"	  TEXT NOT NULL UNIQUE,
                "{self.datetime}"	TEXT NOT NULL,
                "{NewsWebsite.foreign_id}"	INTEGER,
                FOREIGN KEY("{NewsWebsite.foreign_id}") REFERENCES "{NewsWebsite.name}"("id"),
                PRIMARY KEY("id" AUTOINCREMENT)
            );
        """
        self.transaction(transaction)
    
    def add_metadata(self, metadata: ArticleMetadata):
        nw = NewsWebsite(self.connection)
        website_id = nw.get_id(NewsWebsiteEnum.from_url(metadata.url))
        transaction = f"""
            INSERT INTO {self.name} ({self.url}, {self.datetime}, {NewsWebsite.foreign_id})
            VALUES
                (
                    "{self.url_to_str(metadata.url)}",
                    "{self.isoformat(metadata.datetime)}",
                    {website_id}
                )
            ON CONFLICT ({self.url}) DO UPDATE SET {self.datetime}="{self.isoformat(metadata.datetime)}"
        """
        self.transaction(transaction)
    
    def get_id(self, article_url: URL) -> int:
        query = f"""
            SELECT id
            FROM {self.name}
            WHERE
                {self.url} = "{self.url_to_str(article_url)}"
        """
        res = self.query(query)
        row = next(iter(res), None)
        if row is None:
            return 0
        return row[0]

class ArticleTextTable(SqlTable):
    def __init__(self, connection: Connection) -> None:
        super().__init__(connection)
        self.create_table()
    
    @property
    def name(self) -> str:
        return "article_text"

    @property
    def title(self) -> str:
        return "title"

    @property
    def lead(self) -> str:
        return "lead"

    @property
    def body(self) -> str:
        return "body"

    def create_table(self) -> None:
        transaction = f"""
            CREATE TABLE IF NOT EXISTS "{self.name}" (
                "{ArticleMetadataTable.foreign_id}" INTEGER,
                "{self.title}"	               TEXT,
                "{self.lead}"	               TEXT,
                "{self.body}"	               TEXT,
                FOREIGN KEY("{ArticleMetadataTable.foreign_id}") REFERENCES "{ArticleMetadataTable.name}"("id"),
                PRIMARY KEY("{ArticleMetadataTable.foreign_id}")
            );
        """
        self.transaction(transaction)

    def add_article(self, text: ArticleText):
        am = ArticleMetadataTable(self.connection)
        am.add_metadata(text.metadata)
        metadata_id = am.get_id(text.metadata.url)

        transaction = dedent(f"""
            INSERT INTO {self.name}
            VALUES
                (
                    {metadata_id},
                    ?,
                    ?,
                    ?
                )
            ON CONFLICT DO UPDATE SET
                {self.title}=?,
                {self.lead}=?,
                {self.body}=?
        """)
        self.transaction(transaction, (text.title, text.lead, text.body, text.title, text.lead, text.body))
    
    def get_article(self, metadata_id: int) -> Optional[ArticleText]:
        query = f"""
            SELECT 
                {self.name}.{self.title},
                {self.name}.{self.lead},
                {self.name}.{self.body},
                meta.{ArticleMetadataTable.url},
                meta.{ArticleMetadataTable.datetime}
            FROM {self.name}
            JOIN {ArticleMetadataTable.name} AS meta ON {self.name}.{ArticleMetadataTable.foreign_id}=meta.id
            WHERE {self.name}.{ArticleMetadataTable.foreign_id}={metadata_id}
        """
        res = next(iter(self.query(query)), None)
        if res is None:
            return None 
        url = URL(res[3])
        date_and_time = datetime.fromisoformat(res[4])
        article_metadata = DefaultArticleMetadata(url=url, datetime=date_and_time)
        return DefaultArticleText(title=res[0], lead=res[1], body=res[2], metadata=article_metadata)
        
        