from __future__ import annotations

from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

TRACKING_KEYS_PREFIXES = ("utm_",)
TRACKING_KEYS_EXACT = {"fbclid", "gclid", "mc_cid", "mc_eid"}


def normalize_url(url: str) -> str:
    try:
        p = urlparse(url)
    except Exception:
        return url

    query_items = []
    for k, v in parse_qsl(p.query, keep_blank_values=True):
        kl = k.lower()
        if kl in TRACKING_KEYS_EXACT:
            continue
        if any(kl.startswith(pref) for pref in TRACKING_KEYS_PREFIXES):
            continue
        query_items.append((k, v))

    new_query = urlencode(query_items, doseq=True)
    new_p = p._replace(fragment="", query=new_query)
    return urlunparse(new_p)
