"""Label words for clusters, in any language.

A cluster's words are the ones its posts use far more than other posts in the same
language (a per-language background), so an all-French cluster isn't labelled
"les, des, est". URLs, domains and handles are stripped first, stopwords-iso lists
are applied on top, and words are counted once per post so one repetitive post
can't dominate. Japanese and Chinese have no spaces, so they are tokenized as
katakana runs and kanji runs (hiragana is mostly grammar).
"""

from __future__ import annotations

import collections
import re

import numpy as np
import scipy.sparse as sp
import stopwordsiso
from sklearn.feature_extraction.text import CountVectorizer

URLISH = re.compile(r"https?://\S+|\bwww\.\S+|\b[\w-]+(?:\.[\w-]+)*\.(?:com|org|net|social|app|co|jp|io|me|tv|be|ly|gl|gy|fm|uk|de|fr|br)\b\S*"
                    r"|@[\w.-]+|\bbsky\b|\bsocial\b|\bprofile\b", re.I)
TOKEN = (r"[゠-ヿㇰ-ㇿ]{2,}"  # katakana
         r"|[一-鿿]{2,}"  # kanji / hanzi
         r"|[가-힯]{2,}"  # hangul
         r"|(?:(?![぀-ヿ一-鿿가-힯])[^\W\d_]){3,}")  # other scripts, 3+ letters
STOP_EXTRA = {"https", "http", "www", "com", "amp", "lol", "just", "like", "don", "doesn", "didn", "isn", "wasn",
              "aren", "couldn", "wouldn", "shouldn", "ll", "ve", "re", "gonna"}


def clean(text: str | None) -> str:
    return URLISH.sub(" ", text or "")


def lang_of(langs: list[str] | None) -> str:
    return (langs[0].split("-")[0].lower() if langs else "?") or "?"


def _stop(lang: str) -> set[str]:
    words = set(stopwordsiso.stopwords("en")) | STOP_EXTRA
    if stopwordsiso.has_lang(lang):
        words |= set(stopwordsiso.stopwords(lang))
    return words


def label_words(clusters: list[list[str]], cluster_lang: list[str], background: dict[str, list[str]],
                top: int = 10, min_docs: int = 3) -> list[list[str]]:
    """Top words per cluster against the background of the cluster's dominant language (or all of it)."""
    bg_langs = sorted(background)
    docs = [clean(d) for c in clusters for d in c] + [clean(d) for lang in bg_langs for d in background[lang]]
    cv = CountVectorizer(token_pattern=TOKEN, lowercase=True, binary=True, min_df=min_docs, max_features=300_000,
                         dtype=np.float32)
    x = sp.csr_matrix(cv.fit_transform(docs))
    vocab = np.array(cv.get_feature_names_out())

    # Row ranges: clusters first, then each background language.
    bounds, at = [], 0
    for c in clusters:
        bounds.append((at, at + len(c)))
        at += len(c)
    bg_rows = {}
    for lang in bg_langs:
        bg_rows[lang] = (at, at + len(background[lang]))
        at += len(background[lang])
    every = np.asarray(x.sum(axis=0)).ravel() + 1
    every_p = every / every.sum()
    bg_p = {}
    for lang, (a, b) in bg_rows.items():
        counts = np.asarray(x[a:b].sum(axis=0)).ravel() + 1
        bg_p[lang] = counts / counts.sum() if b - a >= 2000 else every_p  # too little text: use everything

    out = []
    for (a, b), lang in zip(bounds, cluster_lang):
        if b == a:
            out.append([])
            continue
        counts = np.asarray(x[a:b].sum(axis=0)).ravel()
        p = (counts + 0.01) / (counts.sum() + 0.01 * len(counts))
        score = p * np.log(p / bg_p.get(lang, every_p))
        score[counts < min_docs] = -np.inf
        stop = _stop(lang)
        words = []
        for i in np.argsort(-score):
            if not np.isfinite(score[i]) or len(words) >= top:
                break
            if vocab[i] not in stop:
                words.append(vocab[i])
        out.append(words)
    return out


def background_sample(texts: list[str], langs: list[str], per_lang: int = 50_000, seed: int = 0) -> dict[str, list[str]]:
    """Up to per_lang texts for each language."""
    by = collections.defaultdict(list)
    for t, lang in zip(texts, langs):
        by[lang].append(t)
    rng = np.random.default_rng(seed)
    return {lang: [v[i] for i in rng.choice(len(v), size=min(per_lang, len(v)), replace=False)]
            for lang, v in by.items() if len(v) >= 500}
