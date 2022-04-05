from unittest.mock import MagicMock, create_autospec

from news_scraper.article.article import DefaultArticleText, NormalizedArticleText
from news_scraper.article.protocols import ArticleMetadata


class TestNormalizedArticleText:
    def test_text_is_normalized_when_retrieved(self):
        metadata = create_autospec(ArticleMetadata, instance=True)

        text_normalizer = MagicMock(side_effect=lambda x: x + "...was normalized")
        default_article = DefaultArticleText(
            title="My Title", lead="My Lead", body="My Body", metadata=metadata
        )
        normalized_article = NormalizedArticleText.from_article_text(
            default_article, text_normalizer=text_normalizer
        )

        assert normalized_article.title == "My Title...was normalized"
        assert normalized_article.lead == "My Lead...was normalized"
        assert normalized_article.body == "My Body...was normalized"
