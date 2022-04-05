from dataclasses import dataclass
from datetime import datetime, timezone
import re
from playwright.sync_api import sync_playwright


@dataclass
class Day:
    year: int
    month: int
    day: int
    hour: int = 0
    minute: int = 0
    second: int = 0

    @property
    def date(self) -> str:
        return datetime(
            self.year, self.month, self.day, self.hour, self.minute, self.second
        ).date()

    @property
    def epoch_delta(self) -> int:
        return int(
            datetime(
                self.year,
                self.month,
                self.day,
                self.hour,
                self.minute,
                self.second,
                tzinfo=timezone.utc,
            ).timestamp()
        )


@dataclass
class ArticleMetadata:
    datetime: int
    url: str


@dataclass
class ArticleText:
    title: str
    lead: str
    body: str

    metadata: ArticleMetadata


def get_tass_article_data(url: str):
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(url)

        # Get the HTML <div> that holds the article
        article_div = page.locator(".news")

        # Get the unix time when the article was created
        datetime = int(article_div.locator("dateformat").get_attribute("time"))
        assert isinstance(datetime, int)
        metadata = ArticleMetadata(datetime=datetime, url=url)

        # Get the different pieces of text in the article
        title = article_div.locator(".news-header__title").inner_text()
        lead = article_div.locator(".news-header__lead").inner_text()
        body = article_div.locator(".text-content").all_inner_texts()
        body = re.sub("\n", " ", re.sub("\n\n", "\n", "".join(body)))

        return ArticleText(title=title, lead=lead, body=body, metadata=metadata)


res = get_tass_article_data("https://tass.com/politics/1383185")
print(res)


"""
Vaccine (Vaxxer for anti-vax)
Coronavirus
Covid-19
Putin
Biden
President
Prime Minister
QR
Sputnik
inoculate
omicron
pfizer
moderna
j&j
who (WHO or w.h.o or World Health Organization)
pandemic
russia
moscow
united states (US)

lenta.ru
tass.ru
ria.ru
themoscowtimes.com
novayagzeta.ru
"""