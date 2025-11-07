import os
import json
import time
import logging
from typing import Dict, Optional
import requests
from requests.models import Response

def make_session() -> requests.Session:
    s = requests.Session()
    token = os.environ.get("GITHUB_TOKEN")
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "contrib-scraper/1.0"
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    s.headers.update(headers)
    return s

def save_json(obj, path: str):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def get_json_with_retry(session: requests.Session, url: str, params: Optional[Dict]=None, max_retries: int=6, backoff: float=2.0, logger: Optional[logging.Logger]=None):
    """GET a JSON endpoint with simple retry (useful for /stats endpoints which may return 202)."""
    for attempt in range(max_retries):
        r = session.get(url, params=params, timeout=60)
        if r.status_code == 200:
            return r.json()
        elif r.status_code == 202:
            # Stats being generated — wait and retry
            if logger: logger.info("202 Accepted (processing). Sleeping before retry...")
            time.sleep(backoff * (attempt + 1))
        else:
            msg = f"GitHub request failed: {r.status_code} {r.text[:200]}"
            if logger: logger.warning(msg)
            r.raise_for_status()
    raise RuntimeError(f"Max retries reached for {url}")


def request_with_rate_limit(session: requests.Session, method: str, url: str, params: Optional[Dict]=None, headers: Optional[Dict]=None, timeout: int=60, max_retries: int=6, backoff: float=1.5, logger: Optional[logging.Logger]=None) -> Response:
    """Make an HTTP request using the provided session and respect GitHub rate-limit headers.

    Behavior:
    - On 200/201/204 return the Response.
    - On 202 (processing) wait and retry (like get_json_with_retry).
    - On 429 or 403 inspect Retry-After or X-RateLimit-Reset and sleep until reset before retrying.
    - For 404 return the Response so callers can handle missing resources.
    - For other 4xx/5xx, retry with exponential backoff up to max_retries then raise for status.
    """
    attempt = 0
    while attempt < max_retries:
        attempt += 1
        try:
            r = session.request(method, url, params=params, headers=headers, timeout=timeout)
        except Exception as e:
            if logger:
                logger.warning(f"Request exception {e} for {url}, attempt {attempt}")
            # backoff then retry
            time.sleep(backoff * attempt)
            continue

        status = r.status_code
        # Success
        if status in (200, 201, 204):
            return r

        # Processing (e.g., contributor stats)
        if status == 202:
            if logger: logger.info(f"202 Accepted for {url} — processing, retrying after backoff")
            time.sleep(backoff * attempt)
            continue

        # Rate limiting or forbidden: try to honor Retry-After or X-RateLimit-Reset
        if status in (429, 403):
            ra = r.headers.get("Retry-After")
            reset = r.headers.get("X-RateLimit-Reset")
            remaining = r.headers.get("X-RateLimit-Remaining")
            if logger:
                logger.warning(f"Rate limit response {status} for {url}; remaining={remaining} Retry-After={ra} reset={reset}")
            wait = None
            if ra:
                try:
                    wait = int(ra)
                except Exception:
                    wait = None
            if wait is None and reset:
                try:
                    reset_ts = int(reset)
                    now = int(time.time())
                    wait = max(0, reset_ts - now) + 2
                except Exception:
                    wait = None
            # fallback exponential backoff
            if wait is None:
                wait = min(60, int(backoff * attempt))
            if wait > 0:
                if logger: logger.info(f"Sleeping {wait}s due to rate-limit for {url}")
                time.sleep(wait)
                continue

        # Not found: return so caller can skip gracefully
        if status == 404:
            if logger: logger.info(f"404 Not Found for {url}")
            return r

        # Other client/server errors: backoff and retry
        if 400 <= status < 600:
            if logger: logger.warning(f"Request failed {status} for {url}, attempt {attempt}")
            time.sleep(backoff * attempt)
            continue

        # Unknown: return response
        return r

    # If we exhausted retries, raise last response or generic error
    try:
        r.raise_for_status()
    except Exception:
        raise RuntimeError(f"Max retries reached for {url}")
