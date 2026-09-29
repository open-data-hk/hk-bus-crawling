import asyncio
import json
import logging
import os

import httpx

try:
    from .schemas import ProviderRoute, ProviderStop
    from .utils import DATA_DIR
except ImportError:
    from schemas import ProviderRoute, ProviderStop
    from utils import DATA_DIR

logger = logging.getLogger(__name__)


class RetriesExhaustedError(Exception):
    """Raised by emitRequest when max_retries is set and every attempt failed."""


async def emitRequest(
    url: str,
    client: httpx.AsyncClient,
    headers={},
    max_retries: int | None = None,
):
    """GET url, retrying transient failures with exponential backoff.

    By default retries forever. With max_retries set, 5xx responses are also
    retried, and RetriesExhaustedError is raised after max_retries retries.
    """
    RETRY_TIMEOUT_MAX = 300
    retry_timeout = 1
    retries = 0

    async def backoff(reason: str):
        nonlocal retry_timeout, retries
        if max_retries is not None and retries >= max_retries:
            raise RetriesExhaustedError(f"{reason} after {retries} retries. URL={url}")
        retries += 1
        logger.warning(f"{reason}, wait {retry_timeout} and retry. URL={url}")
        await asyncio.sleep(retry_timeout)
        retry_timeout = min(retry_timeout * 2, RETRY_TIMEOUT_MAX)

    # retry if "Too many request (429)"
    while True:
        try:
            r = await client.get(url, headers=headers)
            if r.status_code == 200:
                return r
            elif r.status_code in (429, 502, 504, 403) or (
                max_retries is not None and r.status_code >= 500
            ):
                await backoff(f"status_code={r.status_code}")
            else:
                r.raise_for_status()
                raise Exception(r.status_code, url)
        except (httpx.PoolTimeout, httpx.ReadTimeout, httpx.ReadError) as e:
            await backoff(f"Exception {repr(e)} occurred")


GITHUB_API_URL = "https://api.github.com"


async def notify_github_issue(
    title: str, label: str, body: str, client: httpx.AsyncClient
) -> None:
    """Best-effort: file (or comment on the open) GitHub issue with this title.

    Never raises, so a notification failure cannot mask the original error.
    """
    token = os.environ.get("GITHUB_TOKEN")
    repo = os.environ.get("GITHUB_REPOSITORY")
    if not token or not repo:
        logger.error(
            "GITHUB_TOKEN/GITHUB_REPOSITORY not set, skipping GitHub issue "
            "notification %r:\n%s",
            title,
            body,
        )
        return

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
    }
    try:
        r = await client.get(
            f"{GITHUB_API_URL}/search/issues",
            headers=headers,
            params={
                "q": f'repo:{repo} type:issue state:open label:"{label}" '
                f'in:title "{title}"'
            },
        )
        r.raise_for_status()
        existing_issues = r.json().get("items", [])

        if existing_issues:
            issue_number = existing_issues[0]["number"]
            r = await client.post(
                f"{GITHUB_API_URL}/repos/{repo}/issues/{issue_number}/comments",
                headers=headers,
                json={"body": body},
            )
            r.raise_for_status()
            logger.warning("Added comment to existing issue #%s", issue_number)
        else:
            r = await client.post(
                f"{GITHUB_API_URL}/repos/{repo}/issues",
                headers=headers,
                json={"title": title, "body": body, "labels": [label]},
            )
            r.raise_for_status()
            logger.warning("Filed issue %s", r.json().get("html_url"))
    except Exception:
        logger.exception("Failed to file GitHub issue %r:\n%s", title, body)


def get_request_limit():
    default_limit = "10"
    return int(os.environ.get("REQUEST_LIMIT", default_limit))


def store_version(key: str, version: str):
    logger.info(f"{key} version: {version}")
    # "0" is prepended in filename so that this file appears first in Github directory listing
    try:
        with open(DATA_DIR / "0versions.json", "r") as f:
            version_dict = json.load(f)
    except BaseException:
        version_dict = {}
    version_dict[key] = version
    version_dict = dict(sorted(version_dict.items()))
    with open(DATA_DIR / "0versions.json", "w", encoding="UTF-8") as f:
        json.dump(version_dict, f, ensure_ascii=False)


def dump_provider_data(
    co: str,
    route_list: list[ProviderRoute],
    stop_list: dict[str, ProviderStop],
) -> None:
    with open(DATA_DIR / f"routeList.{co}.json", "w", encoding="UTF-8") as f:
        json.dump(route_list, f, ensure_ascii=False)
    with open(DATA_DIR / f"stopList.{co}.json", "w", encoding="UTF-8") as f:
        json.dump(stop_list, f, ensure_ascii=False)
