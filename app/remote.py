"""HTTP client for remote center-imaging manifest and file endpoints.

Contract of the remote source (also implemented locally by ``source_app``):

* ``GET {base_url}/manifest?study_uid=...&cursor=...&page_size=...`` returns
  ``{"study_uid", "page_size", "next_cursor", "items":[{"sop_uid",
  "file_url"}]}``; an empty ``next_cursor`` marks the end of the study.
* ``GET {base_url}/objects/{object_id}`` (or any absolute/relative
  ``file_url`` from the manifest) streams the Explicit VR Little Endian DICOM
  bytes.

The module performs only network I/O and lightweight JSON validation; all
DICOM validation stays in :mod:`app.dicom_io` and all persistence in
:mod:`app.service`.
"""
from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urljoin

import httpx

from .config import BACKFILL_HTTP_TIMEOUT_S, BACKFILL_MAX_PAGE_SIZE


class RemoteSourceError(Exception):
    """Raised when the remote source cannot be read or replies incorrectly.

    ``code`` is a short stable identifier surfaced through the API so callers
    can distinguish transport failures from malformed payloads.
    """

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass
class ManifestItem:
    sop_uid: str
    file_url: str


@dataclass
class ManifestPage:
    items: list[ManifestItem]
    next_cursor: str


def _join(base_url: str, link: str) -> str:
    """Resolve a manifest ``file_url`` against the configured base URL."""
    if link.startswith(("http://", "https://")):
        return link
    return urljoin(base_url.rstrip("/") + "/", link.lstrip("/"))


async def fetch_manifest_page(
    client: httpx.AsyncClient,
    base_url: str,
    study_uid: str,
    cursor: str,
    page_size: int,
) -> ManifestPage:
    url = base_url.rstrip("/") + "/manifest"
    try:
        response = await client.get(
            url,
            params={
                "study_uid": study_uid,
                "cursor": cursor,
                "page_size": page_size,
            },
        )
    except httpx.HTTPError as exc:
        raise RemoteSourceError(
            "manifest_unreachable", f"manifest request failed: {exc}"
        ) from exc
    if response.status_code != 200:
        raise RemoteSourceError(
            "manifest_http_error",
            f"manifest endpoint returned HTTP {response.status_code}",
        )
    try:
        payload = response.json()
    except ValueError as exc:
        raise RemoteSourceError(
            "manifest_bad_json", "manifest response is not valid JSON"
        ) from exc

    try:
        raw_items = payload["items"]
        next_cursor = str(payload.get("next_cursor") or "")
        remote_study_uid = str(payload.get("study_uid") or "")
    except (KeyError, TypeError) as exc:
        raise RemoteSourceError(
            "manifest_malformed", f"manifest payload missing field: {exc}"
        ) from exc

    if remote_study_uid and remote_study_uid != study_uid:
        raise RemoteSourceError(
            "study_uid_mismatch",
            f"manifest study_uid {remote_study_uid!r} does not match requested "
            f"{study_uid!r}",
        )
    if not isinstance(raw_items, list):
        raise RemoteSourceError(
            "manifest_malformed", "manifest 'items' must be a list"
        )
    if len(raw_items) > BACKFILL_MAX_PAGE_SIZE:
        raise RemoteSourceError(
            "manifest_page_too_large",
            f"manifest page has {len(raw_items)} items; limit is "
            f"{BACKFILL_MAX_PAGE_SIZE}",
        )

    items: list[ManifestItem] = []
    for index, raw in enumerate(raw_items):
        try:
            sop_uid = str(raw["sop_uid"]).strip()
            file_url = str(raw["file_url"]).strip()
        except (KeyError, TypeError) as exc:
            raise RemoteSourceError(
                "manifest_malformed",
                f"manifest item {index} missing field: {exc}",
            ) from exc
        if not sop_uid or not file_url:
            raise RemoteSourceError(
                "manifest_malformed",
                f"manifest item {index} has empty sop_uid or file_url",
            )
        items.append(ManifestItem(sop_uid=sop_uid, file_url=_join(base_url, file_url)))
    return ManifestPage(items=items, next_cursor=next_cursor)


async def download_object(
    client: httpx.AsyncClient, file_url: str
) -> bytes:
    """Download one DICOM object; transport/HTTP failures stay retryable."""
    try:
        response = await client.get(file_url)
    except httpx.HTTPError as exc:
        raise RemoteSourceError(
            "download_failed", f"download from {file_url} failed: {exc}"
        ) from exc
    if response.status_code != 200:
        raise RemoteSourceError(
            "download_http_error",
            f"download from {file_url} returned HTTP {response.status_code}",
        )
    content = response.content
    if not content:
        raise RemoteSourceError(
            "download_empty", f"download from {file_url} returned no bytes"
        )
    return content


def new_http_client() -> httpx.AsyncClient:
    """Shared client factory: bounded timeouts keep a broken source from
    pinning a worker forever."""
    timeout = httpx.Timeout(BACKFILL_HTTP_TIMEOUT_S, connect=10.0)
    return httpx.AsyncClient(timeout=timeout, follow_redirects=True)
