from yarl import URL
from news_scraper.article.protocols import ArticleText

from news_scraper.article_retriever.tass import TassArticleRetriever


class TestTassArticleRetriever:
    def test_gets_text(self):
        retriever = TassArticleRetriever()
        url = URL("https://tass.com/politics/1433417")

        out = retriever(url)
        assert isinstance(out, ArticleText)
        assert (
            out.title == "LDPR’s interim leader chosen, in wake of Zhirinovsky's death"
        )
        assert (
            out.lead
            == "The source specified that currently the faction is taking a time-out on all organizational and staff decisions"
        )
        assert out.body.endswith(
            "On Wednesday, State Duma Speaker Vyacheslav Volodin reported about his passing."
        )
