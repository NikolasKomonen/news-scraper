from dataclasses import dataclass, field

from yarl import URL
from news_scraper.article.article import NormalizedArticleText
from news_scraper.article.protocols import ArticleText
from news_scraper.article_retriever.protocols import ArticleRetriever

from news_scraper.word.protocols import TextWithAliases
from news_scraper.word.word import NormalizedTextWithAliases

from news_scraper.word_counter.fuzzy_word_counter import FuzzyWordVariantCounter

from news_scraper.word_counter.protocols import (
    ArticleWithWordCount,
    ArticleWordCounter,
    WordCounter,
)


@dataclass(frozen=True)
class DefaultArticleWithWordCount:
    word_with_aliases: TextWithAliases

    title: int
    lead: int
    body: int

    article_text: ArticleText


@dataclass(frozen=True)
class FuzzyArticleWordVariantCounter:

    word_counter: WordCounter = FuzzyWordVariantCounter()
    word_count_obj: ArticleWithWordCount = DefaultArticleWithWordCount

    def __call__(
        self, article: ArticleText, words: TextWithAliases
    ) -> ArticleWithWordCount:
        title = self.word_counter(article.title, words)
        lead = self.word_counter(article.lead, words)
        body = self.word_counter(article.body, words)
        return self.word_count_obj(words, title, lead, body, article)


@dataclass(frozen=True)
class WordCounterFromArticle:

    article_retriever: ArticleRetriever
    article_word_counter: ArticleWordCounter = field(
        default_factory=FuzzyArticleWordVariantCounter
    )

    def __call__(
        self, url: URL, words: NormalizedTextWithAliases
    ) -> ArticleWithWordCount:
        article = self.article_retriever(url)
        normalized_article = NormalizedArticleText.from_article_text(article)
        return self.article_word_counter(normalized_article, words)
