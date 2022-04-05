from yarl import URL
from news_scraper.article.article import NormalizedArticleText
from news_scraper.article_retriever.tass import TassArticleRetriever
from news_scraper.word.word import NormalizedTextWithAliases
from news_scraper.word_counter.fuzzy_word_counter import FuzzyArticleWordVariantCounter


def test_main():
    url = URL("https://tass.com/society/1433275")
    tass_article_retriever = TassArticleRetriever()
    article = tass_article_retriever(url)
    normalized_article = NormalizedArticleText.from_article_text(article)

    fuzzy_article_word_counter = FuzzyArticleWordVariantCounter()
    covid_words = NormalizedTextWithAliases("covid", ["covid19"])

    out = fuzzy_article_word_counter(normalized_article, covid_words)

    assert out.title == 1
    assert out.lead == 1
    assert out.body == 9
    assert out.article_text.title.startswith("russias covid19 cases surge")
    assert out.article_text.lead.startswith("moscows covid19 cases surged 1072")
    assert out.article_text.body.startswith("moscow april 6 tass russias covid19")
    assert out.article_text.body.endswith(
        "average mortality rate remained 207 according crisis center"
    )
