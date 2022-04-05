from dataclasses import dataclass, field
from typing import Optional

from yarl import URL

from news_scraper.article.protocols import ArticleMetadata, ArticleText
from news_scraper.text_normalizer.nltk_normalizer import NltkStringNormalizer
from news_scraper.text_normalizer.protocols import StringNormalizer
from datetime import datetime


@dataclass(frozen=True)
class DefaultArticleMetadata:

    datetime: datetime
    url: URL


@dataclass(frozen=True)
class DefaultArticleText:

    title: str
    lead: str
    body: str

    metadata: ArticleMetadata


@dataclass(frozen=True)
class NormalizedArticleText:

    title: str
    lead: str
    body: str

    metadata: ArticleMetadata

    @classmethod
    def from_article_text(
        cls, article: ArticleText, text_normalizer: Optional[StringNormalizer] = None
    ):
        if text_normalizer is None:
            text_normalizer = NltkStringNormalizer()
        n = text_normalizer
        return cls(n(article.title), n(article.lead), n(article.body), article.metadata)

    # @property
    # def title(self) -> str:
    #     if getattr(self, "normalized_title", None) is None:
    #         self.normalized_title = self.text_normalizer(self._title)
    #     return self.normalized_title

    # @property
    # def lead(self) -> str:
    #     if getattr(self, "normalized_lead", None) is None:
    #         self.normalized_lead = self.text_normalizer(self._lead)
    #     return self.normalized_lead

    # @property
    # def body(self) -> str:
    #     if getattr(self, "normalized_body", None) is None:
    #         self.normalized_body = self.text_normalizer(self._body)
    #     return self.normalized_body
