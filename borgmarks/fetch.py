from __future__ import annotations

import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup  # type: ignore

from .log import get_logger

log = get_logger(__name__)


@dataclass
class FetchResult:
    ok: bool
    status: Optional[int]
    final_url: Optional[str]
    title: Optional[str]
    description: Optional[str]
    snippet: Optional[str]
    favicon_url: Optional[str]
    html: Optional[str]
    fetch_ms: int
    error: Optional[str] = None


def fetch_many(
    urls: List[str],
    *,
    backend: str,
    jobs: int,
    timeout_s: int,
    user_agent: str,
    max_bytes: int,
) -> Dict[str, FetchResult]:
    """Fetch many URLs and extract a small snippet.

    backends:
      - httpx: fetch body (title/description/snippet)
      - curl: subprocess-based, status + final_url only (v0.7.5)
    """
    backend = backend.lower()
    if backend == "curl":
        return _fetch_many_curl(urls, jobs=jobs, timeout_s=timeout_s, user_agent=user_agent)
    return _fetch_many_httpx(urls, jobs=jobs, timeout_s=timeout_s, user_agent=user_agent, max_bytes=max_bytes)


def _fetch_many_httpx(
    urls: List[str],
    *,
    jobs: int,
    timeout_s: int,
    user_agent: str,
    max_bytes: int,
) -> Dict[str, FetchResult]:
    out: Dict[str, FetchResult] = {}
    timeout = httpx.Timeout(timeout_s, connect=timeout_s)
    headers = {"User-Agent": user_agent}

    def _one(url: str) -> Tuple[str, FetchResult]:
        t0 = time.time()
        try:
            with httpx.Client(follow_redirects=True, headers=headers, timeout=timeout) as client:
                r = client.get(url)
                content = r.content[:max_bytes]
                title, desc, snippet, favicon = _extract_meta(content, base_url=str(r.url))
                ms = int((time.time() - t0) * 1000)
                return url, FetchResult(
                    ok=(200 <= r.status_code < 400),
                    status=r.status_code,
                    final_url=str(r.url),
                    title=title,
                    description=desc,
                    snippet=snippet,
                    favicon_url=favicon,
                    html=_decode_html(content),
                    fetch_ms=ms,
                    error=None,
                )
        except Exception as e:
            ms = int((time.time() - t0) * 1000)
            return url, FetchResult(
                ok=False,
                status=None,
                final_url=None,
                title=None,
                description=None,
                snippet=None,
                favicon_url=None,
                html=None,
                fetch_ms=ms,
                error=str(e),
            )

    with ThreadPoolExecutor(max_workers=max(1, jobs)) as ex:
        futs = [ex.submit(_one, u) for u in urls]
        for fut in as_completed(futs):
            url, res = fut.result()
            out[url] = res
    return out


def _fetch_many_curl(
    urls: List[str],
    *,
    jobs: int,
    timeout_s: int,
    user_agent: str,
) -> Dict[str, FetchResult]:
    """curl backend (subprocess + threadpool).

    Notes:
    - This backend only captures status + final_url (no page snippet) in v0.7.5.
    - It's mostly here for compatibility with environments where httpx is blocked.
    """
    out: Dict[str, FetchResult] = {}

    def _one(url: str) -> Tuple[str, FetchResult]:
        t0 = time.time()
        try:
            # status + effective URL in one call
            # (curl prints status then effective url)
            cmd = [
                "curl",
                "-L",
                "--max-time",
                str(timeout_s),
                "-A",
                user_agent,
                "-o",
                "/dev/null",
                "-sS",
                "-w",
                "%{http_code}\t%{url_effective}",
                url,
            ]
            r = subprocess.run(cmd, capture_output=True, text=True)
            ms = int((time.time() - t0) * 1000)
            if r.returncode != 0:
                return url, FetchResult(
                    ok=False, status=None, final_url=None,
                    title=None, description=None, snippet=None, favicon_url=None, html=None,
                    fetch_ms=ms, error=(r.stderr.strip() or f"curl rc={r.returncode}")
                )
            parts = (r.stdout or "").split("\t", 1)
            code_s = parts[0].strip() if parts else ""
            eff = parts[1].strip() if len(parts) > 1 else None
            status = int(code_s) if code_s.isdigit() else None
            ok = status is not None and 200 <= status < 400
            return url, FetchResult(
                ok=ok,
                status=status,
                final_url=eff or None,
                title=None, description=None, snippet=None, html=None,
                favicon_url=None,
                fetch_ms=ms,
                error=None if ok else "http_status_not_ok",
            )
        except Exception as e:
            ms = int((time.time() - t0) * 1000)
            return url, FetchResult(
                ok=False, status=None, final_url=None,
                title=None, description=None, snippet=None, favicon_url=None, html=None,
                fetch_ms=ms, error=str(e)
            )

    with ThreadPoolExecutor(max_workers=max(1, jobs)) as ex:
        futs = [ex.submit(_one, u) for u in urls]
        for fut in as_completed(futs):
            url, res = fut.result()
            out[url] = res
    return out


def _extract_meta(content: bytes, *, base_url: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    if not content:
        return None, None, None, None
    try:
        soup = BeautifulSoup(content, "lxml")
        title = soup.title.get_text(strip=True) if soup.title else None
        desc = None
        m = soup.find("meta", attrs={"name": "description"})
        if m and m.get("content"):
            desc = m.get("content").strip()

        parts: List[str] = []
        for p in soup.find_all("p"):
            t = p.get_text(" ", strip=True)
            if t:
                parts.append(t)
            if sum(len(x) for x in parts) > 1200:
                break
        snippet = " ".join(parts)
        snippet = snippet[:1500] if snippet else None
        favicon = _extract_favicon_url(soup, base_url)
        return title, desc, snippet, favicon
    except Exception:
        return None, None, None, None


def _extract_favicon_url(soup, base_url: str) -> Optional[str]:
    # Prefer explicit icon declarations.
    icon_rels = {"icon", "shortcut icon", "apple-touch-icon", "mask-icon"}
    for link in soup.find_all("link"):
        rel = " ".join([x.lower() for x in (link.get("rel") or [])]) if link.get("rel") else ""
        href = (link.get("href") or "").strip()
        if not href:
            continue
        if rel in icon_rels or "icon" in rel:
            return urljoin(base_url, href)
    # Fallback to conventional favicon location.
    try:
        from urllib.parse import urlparse

        p = urlparse(base_url)
        if p.scheme and p.netloc:
            return f"{p.scheme}://{p.netloc}/favicon.ico"
    except Exception:
        pass
    return None


def _decode_html(content: bytes) -> Optional[str]:
    if not content:
        return None
    try:
        return content.decode("utf-8", errors="replace")
    except Exception:
        return None
