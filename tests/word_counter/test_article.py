from unittest.mock import create_autospec
from news_scraper.article.protocols import ArticleText
from news_scraper.word.protocols import TextWithAliases
from news_scraper.word_counter.article import FuzzyArticleWordVariantCounter

from news_scraper.word_counter.protocols import ArticleWithWordCount, WordCounter


class TestFuzzyArticleWordVariantCounter:
    def test_main(self):
        actual_counter = create_autospec(WordCounter, instance=True)
        actual_counter.return_value = 2
        article_counter = FuzzyArticleWordVariantCounter(actual_counter)

        article = create_autospec(ArticleText, instance=True)
        words = create_autospec(TextWithAliases, instance=True)

        out = article_counter(article=article, words=words)
        assert isinstance(out, ArticleWithWordCount)
        assert out.title == 2
        assert out.lead == 2
        assert out.body == 2
        assert out.word_with_aliases == words
        assert out.article_text == article
