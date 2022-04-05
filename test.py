# # from datetime import datetime, timezone
# # from yarl import URL
# # from news_scraper.article_retriever.database import DatabaseArticleRetriever

# # from news_scraper.database.mongo.client import Mongo


# # def main():
# #     mongo = Mongo().client
# #     db = mongo.get_database("test_db")

# #     collection = db.get_collection("article_text")
# #     collection.delete_many({})
# #     collection.insert_one(
# #         {
# #             "title": "This is the title",
# #             "lead": "This is the lead",
# #             "body": "This is my body",
# #             "metadata": {
# #                 "datetime": datetime.now(tz=timezone.utc).isoformat(),
# #                 "url": URL("https://my.dog.com").human_repr()
# #             },
# #         }
# #     )

# #     dbar = DatabaseArticleRetriever(db)
# #     print(dbar(URL("https://my.dog.com")))

# # if __name__ == "__main__":
# #     main()

# from datetime import datetime, timezone

# import arrow
# import pytz

# datetime_str = "08:00 14/04/2022"
# try:
#     datetime_obj = arrow.get(datetime_str, "HH:mm DD.MM.YYYY", locale="ru")
# except arrow.parser.ParserMatchError:
#     ru = pytz.timezone("Europe/Moscow")
#     datetime_obj = arrow.get(datetime_str, "HH:mm DD/MM/YYYY", tzinfo=ru)


# print(datetime_obj)

from yarl import URL
from news_scraper.database.sqlite.client import (
    NewsScraperSqliteConnection,
    WaybackResult,
)

with NewsScraperSqliteConnection.connection() as conn:
    wr = WaybackResult(conn)
    wr.get_id_from_wayback_url(
        URL("http://web.archive.org/web/20211201121045/https://tass.com/")
    )


"""

INSERT INTO article_metadata (url, datetime, news_site_id)         
VALUES              
(                  
"http://web.archive.org/web/20211129080435/https://tass.com/defense/1366927",
"2021-11-26 08:34:18",
6
)
ON CONFLICT (url) DO UPDATE SET datetime="2021-11-26 08:34:18"\n        '
"""