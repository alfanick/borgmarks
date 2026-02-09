from __future__ import annotations

from urllib.parse import urlparse

import tldextract  # type: ignore

TLD_LANG = {
    "pl": "PL",
    "de": "DE",
    "at": "DE",
    "fr": "FR",
    "it": "IT",
    "es": "ES",
    "pt": "PT",
    "cz": "CS",
    "sk": "SK",
    "hu": "HU",
    "ro": "RO",
    "ru": "RU",
    "ua": "UK",
    "jp": "JA",
    "kr": "KO",
    "cn": "ZH",
    "tw": "ZH",
    "se": "SV",
    "no": "NO",
    "dk": "DA",
    "fi": "FI",
    "nl": "NL",
}

DO_NOT_MAP_TLDS = {"ch", "com", "org", "net", "io", "app", "dev", "ai"}


def domain_of(url: str) -> str:
    try:
        p = urlparse(url)
        host = p.netloc
    except Exception:
        return ""
    if host.startswith("www."):
        host = host[4:]
    return host


def guess_lang(url: str, title: str) -> str:
    ext = tldextract.extract(url)
    tld = ext.suffix.split(".")[-1].lower() if ext.suffix else ""
    if tld and tld not in DO_NOT_MAP_TLDS and tld in TLD_LANG:
        return TLD_LANG[tld]

    t = title.lower()
    if any(ch in t for ch in "ąćęłńóśżź"):
        return "PL"
    if any(ch in t for ch in "äöüß"):
        return "DE"
    if any(ch in t for ch in "àâçéèêëîïôœùûüÿ"):
        return "FR"
    return "EN"
