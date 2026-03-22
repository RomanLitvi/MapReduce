"""
Text tokenizer for inverted index.
Handles Unicode, lowercasing, and basic normalization.
"""
import re

# Precompile for performance — matches sequences of word characters
_TOKEN_RE = re.compile(r"[a-zA-Zа-яА-ЯіІїЇєЄґҐ0-9]+", re.UNICODE)

# Common stop words (English) — skip to reduce index size
_STOP_WORDS = frozenset({
    "a", "an", "the", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "is", "it", "be", "as", "by", "was", "are", "with", "this",
    "that", "from", "not", "have", "has", "had", "will", "would", "can",
    "could", "do", "does", "did", "its", "if", "than", "then", "so",
})


def tokenize(text: str) -> list[str]:
    """
    Tokenize text into lowercase terms, filtering stop words and short tokens.
    """
    tokens = _TOKEN_RE.findall(text.lower())
    return [t for t in tokens if len(t) > 1 and t not in _STOP_WORDS]
