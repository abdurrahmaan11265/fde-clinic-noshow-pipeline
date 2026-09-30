"""Step 1: retrieve. Fetch every source, keep the raw bytes untouched, record what came from where.

Raw files are never edited. Each retrieval is recorded in the run manifest with its URL,
timestamp, size and SHA-256, so any number in the outputs can be traced back to exact bytes.
"""
import hashlib
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

from . import config

log = logging.getLogger("pipeline.retrieve")


class RetrievalError(RuntimeError):
    pass


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _get(url, params=None) -> requests.Response:
    """GET with retries on 429/5xx and on network errors. Raises after the last attempt."""
    for attempt in range(1, config.HTTP_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, timeout=config.HTTP_TIMEOUT_S)
            if resp.status_code in config.RETRYABLE_STATUS:
                raise requests.HTTPError(f"retryable HTTP {resp.status_code}", response=resp)
            resp.raise_for_status()
            return resp
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as exc:
            status = getattr(exc.response, "status_code", None)
            if status is not None and status not in config.RETRYABLE_STATUS:
                raise RetrievalError(f"{url} -> HTTP {status}, not retryable") from exc
            if attempt == config.HTTP_RETRIES:
                raise RetrievalError(f"{url} failed after {attempt} attempts: {exc}") from exc
            wait = 2 ** attempt
            log.warning("attempt %d/%d for %s failed (%s); retrying in %ds",
                        attempt, config.HTTP_RETRIES, url, exc, wait)
            time.sleep(wait)


def _latest_snapshot(folder: Path, pattern: str):
    snaps = sorted(folder.glob(pattern))
    return snaps[-1] if snaps else None


def fetch_appointments(offline: bool = False) -> dict:
    """The CSV is a fixed historical extract, so it is content-addressed: download once, verify, reuse."""
    src = config.APPOINTMENTS
    folder = config.RAW / "appointments"
    folder.mkdir(parents=True, exist_ok=True)
    target = folder / f"appointments_{src['sha256'][:12]}.csv"

    if target.exists() and _sha256(target.read_bytes()) == src["sha256"]:
        log.info("appointments: cached raw copy verified (%s)", target.name)
        status = "cached"
    elif offline:
        raise RetrievalError("offline run requested but no verified raw appointments file exists")
    else:
        log.info("appointments: downloading %s", src["url"])
        data = _get(src["url"]).content
        digest = _sha256(data)
        if digest != src["sha256"]:
            # Do not silently accept a different file: quarantine it and stop.
            bad = folder / f"REJECTED_{digest[:12]}.csv"
            bad.write_bytes(data)
            raise RetrievalError(f"appointments hash mismatch: got {digest[:12]}, "
                                 f"expected {src['sha256'][:12]}; saved as {bad.name}")
        target.write_bytes(data)
        status = "fresh"

    data = target.read_bytes()
    return {"source": "appointments", "type": "CSV file", "url": src["url"],
            "canonical": src["canonical"], "path": str(target.relative_to(config.ROOT)),
            "bytes": len(data), "sha256": _sha256(data), "status": status,
            "retrieved_at": datetime.fromtimestamp(target.stat().st_mtime, timezone.utc).isoformat()}


def _fetch_json(name: str, url: str, params: dict, tag: str, offline: bool) -> dict:
    """Fetch a JSON API response and save it verbatim. If the API is down, fall back to the
    newest saved snapshot and mark the source 'stale' instead of failing the whole run."""
    folder = config.RAW / name
    folder.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    status = "fresh"
    try:
        if offline:
            raise RetrievalError("offline run requested")
        resp = _get(url, params=params)
        json.loads(resp.content)  # refuse to store something that is not JSON
        path = folder / f"{name}_{tag}_{stamp}.json"
        path.write_bytes(resp.content)
        log.info("%s: saved %s (%d bytes)", name, path.name, len(resp.content))
    except (RetrievalError, ValueError) as exc:
        path = _latest_snapshot(folder, f"{name}_{tag}_*.json")
        if path is None:
            raise RetrievalError(f"{name}: API unavailable and no saved snapshot: {exc}") from exc
        status = "stale"
        log.warning("%s: using saved snapshot %s because: %s", name, path.name, exc)

    data = path.read_bytes()
    return {"source": name, "type": "JSON API", "url": url, "params": params,
            "path": str(path.relative_to(config.ROOT)), "bytes": len(data),
            "sha256": _sha256(data), "status": status,
            "retrieved_at": datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat()}


def fetch_holidays(years, offline=False) -> list:
    return [_fetch_json("holidays", config.HOLIDAYS["url"].format(year=y), None, str(y), offline)
            for y in sorted(years)]


def fetch_weather(start: str, end: str, offline=False) -> dict:
    params = {**config.WEATHER["params"], "start_date": start, "end_date": end}
    return _fetch_json("weather", config.WEATHER["url"], params, f"{start}_{end}", offline)
