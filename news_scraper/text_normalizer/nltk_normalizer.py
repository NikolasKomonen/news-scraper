from dataclasses import dataclass
from functools import lru_cache
import nltk

nltk.download("stopwords")
from nltk.corpus import stopwords
import re
from news_scraper.text_normalizer.protocols import StringNormalizer

STOPWORDs = stop_words = stopwords.words("english")


@dataclass(frozen=True)
class NltkStringNormalizer(StringNormalizer):
    def __call__(self, text: str) -> str:
        text = text.lower()
        text = re.sub(
            r"(@\[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)|^rt|http.+?", "", text
        )
        text = " ".join([word for word in text.split() if word not in STOPWORDs])
        return text


cached_nltk_string_normalizer = lru_cache()(NltkStringNormalizer())
