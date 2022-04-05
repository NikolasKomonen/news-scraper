import requests
import pandas as pd
import datetime

from yarl import URL
from news_scraper.database.sqlite.client import (
    NewsWebsiteEnum,
    NewsScraperSqliteConnection,
    WaybackResult,
)

websites = [
    "https://www.themoscowtimes.com",
    "https://novayagazeta.ru",
    "https://ria.ru",
    "https://lenta.ru",
    "https://tass.ru",
    "https://tass.com",
]


def date_parser(s):
    date_time_obj = datetime.datetime.strptime(s, "%A, %B %d, %Y at %I:%M:%S %p")
    return date_time_obj


def convert_to_wayback_ts(ts):
    return ts.strftime("%Y%m%d%H%M")


def convert_to_ts(wayback_ts):
    return datetime.datetime.strptime(wayback_ts[0:12], "%Y%m%d%H%M")


def get_wayback_data(requested_website: str, ts: datetime):
    # using wayback API - see https://archive.org/help/wayback_api.php
    ts_text = convert_to_wayback_ts(ts)
    link = f"https://archive.org/wayback/available?url={requested_website}&timestamp={ts_text}"
    print(f"Getting {link}")
    r = requests.get(link, auth=("user", "pass"))
    res = r.json()
    website_url = URL(requested_website)

    data = {
        "domain": NewsWebsiteEnum(website_url.host.lower()),
        "requested_url": URL(requested_website),
        "requested_ts": ts,
    }
    try:
        wayback_url = res["archived_snapshots"]["closest"]["url"]
        wayback_ts = res["archived_snapshots"]["closest"]["timestamp"]
    except:
        return data
    data["actual_url"] = URL(wayback_url)
    data["actual_ts"] = convert_to_ts(wayback_ts)
    return data


def do():
    df = pd.read_csv("datetime.csv")
    timestamps = df["datetime"].apply(date_parser).to_list()

    with NewsScraperSqliteConnection.connection() as conn:
        wayback = WaybackResult(conn)

        for wb in NewsWebsiteEnum:
            wb = "https://" + wb.value
            for ts in timestamps:
                print("---------")
                if wayback.request_exists(URL(wb), ts):
                    print(f"Request for {wb} at {ts} already exists.")
                    continue
                print(f"Getting '{wb}' at '{ts}'")
                wb_data = get_wayback_data(requested_website=wb, ts=ts)
                news_site = wb_data["domain"]
                requested_url = wb_data["requested_url"]
                requested_ts = wb_data["requested_ts"]
                actual_url = wb_data.get("actual_url", None)
                actual_ts = wb_data.get("actual_ts", None)
                wayback.insert_row(
                    news_site, requested_url, requested_ts, actual_url, actual_ts
                )
            # if "wayback_ts" not in wb_data:
            #     print(f"no wb data for request link {wb_data['request_link']}")
            # elif wb_data["wayback_ts"] < ts:
            #     timediff = ts - wb_data["wayback_ts"]
            #     if (
            #         wb == "https://www.themoscowtimes.com"
            #         and timediff > datetime.timedelta(minutes=15)
            #     ) or (wb != "https://www.themoscowtimes.com"):
            #         new_ts = ts
            #         for _ in range(2):
            #             new_ts += datetime.timedelta(minutes=30)
            #             new_wb_data = get_wayback_data(website=wb, ts=new_ts)
            #             if "wayback_ts" not in new_wb_data:
            #                 break
            #             if new_wb_data["wayback_ts"] > ts:
            #                 new_wb_timediff = new_wb_data["wayback_ts"] - ts
            #                 if (
            #                     new_wb_data["wayback_ts"] - ts
            #                     <= datetime.timedelta(minutes=90)
            #                     or new_wb_timediff < timediff
            #                 ):
            #                     wb_data = new_wb_data
            #                 break
            # wb_data["original_ts"] = ts.to_pydatetime()
            # if "wayback_ts" in wb_data:
            #     wb_data["timediff_wb_orig"] = str(
            #         abs(wb_data["wayback_ts"] - wb_data["original_ts"])
            #     )
            #     wb_data["wb_before_or_after"] = (
            #         "after"
            #         if wb_data["wayback_ts"] > wb_data["original_ts"]
            #         else "before"
            #     )

            # collection.update_one(
            #     filter={
            #         "website": wb_data["website"],
            #         "original_ts": wb_data["original_ts"],
            #     },
            #     upsert=True,
            #     update={"$set": wb_data},
            # )
