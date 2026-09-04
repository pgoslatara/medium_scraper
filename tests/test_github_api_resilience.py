"""Tests for GitHub API resilience against auth failures and partial GraphQL data.

These cover the two failure modes that previously either crashed with a bare
TypeError/JSONDecodeError or were misclassified as a rate limit:
  1. An authentication/abuse-protection response (401, or a non-JSON body).
  2. A partial GraphQL response that nulls the parent object or an individual edge.
"""

from typing import Any, Optional

import pytest  # type: ignore[import-not-found]

from utils import utils
from utils.utils import GitHubAPIError, call_github_api, extract_graphql_page

# Sentinel telling FakeResponse to raise on .json(), mimicking a non-JSON body.
_NO_JSON = object()


class FakeResponse:
    """Minimal stand-in for a requests.Response, as used by call_github_api."""

    def __init__(self, status_code: int, payload: Any = None, text: str = "") -> None:
        self.status_code = status_code
        self.reason = "test"
        self.text = text
        self._payload = payload

    def json(self) -> Any:
        if self._payload is _NO_JSON:
            raise ValueError("No JSON object could be decoded")
        return self._payload


def _patch_request(monkeypatch: pytest.MonkeyPatch, response: FakeResponse) -> None:
    def fake_request(**kwargs: Any) -> FakeResponse:
        return response

    monkeypatch.setattr(utils, "request_github_api", fake_request)


def test_call_github_api_fails_fast_on_401(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_request(monkeypatch, FakeResponse(401, {"message": "Bad credentials"}))

    with pytest.raises(GitHubAPIError) as exc_info:
        call_github_api(method="graphql", json={"query": "{ viewer { login } }"})

    assert "401" in str(exc_info.value)


def test_call_github_api_fails_fast_on_non_json_body(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_request(monkeypatch, FakeResponse(401, _NO_JSON, text="<html>blocked</html>"))

    with pytest.raises(GitHubAPIError) as exc_info:
        call_github_api(method="graphql", json={"query": "{ viewer { login } }"})

    message = str(exc_info.value)
    assert "non-JSON" in message
    assert "401" in message


def test_extract_graphql_page_skips_null_edges_and_nodes() -> None:
    payload = {
        "data": {
            "repository": {
                "pullRequests": {
                    "pageInfo": {"endCursor": "cursor-1"},
                    "edges": [{"node": {"number": 1}}, None, {"node": None}],
                }
            }
        }
    }

    nodes, end_cursor = extract_graphql_page(payload, "repository", "pullRequests", "owner/repo")

    assert nodes == [{"number": 1}]
    assert end_cursor == "cursor-1"


def test_extract_graphql_page_returns_empty_when_parent_null() -> None:
    payload = {
        "data": {"repository": None},
        "errors": [{"type": "FORBIDDEN", "message": "no access"}],
    }

    nodes, end_cursor = extract_graphql_page(payload, "repository", "pullRequests", "owner/repo")

    assert nodes == []
    assert end_cursor is None


def test_extract_graphql_page_end_cursor_is_optional_str() -> None:
    payload = {
        "data": {
            "repository": {
                "pullRequests": {
                    "pageInfo": {"endCursor": None},
                    "edges": [{"node": {"number": 2}}],
                }
            }
        }
    }

    _, end_cursor = extract_graphql_page(payload, "repository", "pullRequests", "owner/repo")
    result: Optional[str] = end_cursor

    assert result is None
