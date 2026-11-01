"""The HTTP session blocks use for public services (LANDFIRE, the Iowa Mesonet, hosted
references). Not part of any artifact's identity: retrying doesn't change what is produced."""
from __future__ import annotations

UA = {"User-Agent": "scil-bench (UCSD SCIL wildfire spread-model benchmarking)"}


def session():
    """A requests session that retries: public services time out or return 5xx now and
    then, and a blip shouldn't fail the run. Up to 4 retries per request (timeouts,
    dropped connections, 429/5xx), waiting 10, 20, 40 s in between."""
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    retry = Retry(total=4, connect=4, read=4, status=4, backoff_factor=5,
                  status_forcelist=(429, 500, 502, 503, 504), allowed_methods=("GET", "POST"))
    s = requests.Session()
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    s.headers.update(UA)
    return s
