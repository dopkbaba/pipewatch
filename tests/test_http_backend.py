"""Tests for the HTTP backend using responses (requests mock library)."""

from __future__ import annotations

import datetime
import json

import pytest

responses = pytest.importorskip("responses")
requests = pytest.importorskip("requests")

from pipewatch.backends.http import HttpBackend  # noqa: E402

BASE_URL = "http://pipewatch.test"


@pytest.fixture()
def backend() -> HttpBackend:
    return HttpBackend(base_url=BASE_URL, timeout=5)


class TestHttpBackendListPipelines:
    @responses.activate
    def test_returns_pipeline_ids(self, backend: HttpBackend) -> None:
        responses.add(
            responses.GET,
            f"{BASE_URL}/pipelines",
            json=[{"id": "etl_alpha"}, {"id": "etl_beta"}],
            status=200,
        )
        result = backend.list_pipelines()
        assert result == ["etl_alpha", "etl_beta"]

    @responses.activate
    def test_raises_on_non_200(self, backend: HttpBackend) -> None:
        responses.add(responses.GET, f"{BASE_URL}/pipelines", status=503)
        with pytest.raises(requests.HTTPError):
            backend.list_pipelines()


class TestHttpBackendFetch:
    @responses.activate
    def test_fetch_full_metrics(self, backend: HttpBackend) -> None:
        payload = {
            "id": "etl_alpha",
            "last_run": "2024-01-15T08:00:00+00:00",
            "last_duration_seconds": 120,
            "last_record_count": 5000,
            "last_error": None,
        }
        responses.add(responses.GET, f"{BASE_URL}/pipelines/etl_alpha", json=payload, status=200)
        metrics = backend.fetch("etl_alpha")
        assert metrics.pipeline_id == "etl_alpha"
        assert metrics.last_duration_seconds == 120
        assert metrics.last_record_count == 5000
        assert metrics.last_error is None
        assert metrics.last_run is not None
        assert metrics.last_run.tzinfo is not None

    @responses.activate
    def test_fetch_naive_datetime_becomes_utc(self, backend: HttpBackend) -> None:
        payload = {"id": "etl_beta", "last_run": "2024-01-15T08:00:00", "last_error": None}
        responses.add(responses.GET, f"{BASE_URL}/pipelines/etl_beta", json=payload, status=200)
        metrics = backend.fetch("etl_beta")
        assert metrics.last_run is not None
        assert metrics.last_run.tzinfo == datetime.timezone.utc

    @responses.activate
    def test_fetch_missing_last_run_is_none(self, backend: HttpBackend) -> None:
        payload = {"id": "etl_gamma", "last_run": None, "last_error": "timeout"}
        responses.add(responses.GET, f"{BASE_URL}/pipelines/etl_gamma", json=payload, status=200)
        metrics = backend.fetch("etl_gamma")
        assert metrics.last_run is None
        assert metrics.last_error == "timeout"

    @responses.activate
    def test_fetch_raises_on_404(self, backend: HttpBackend) -> None:
        responses.add(responses.GET, f"{BASE_URL}/pipelines/missing", status=404)
        with pytest.raises(requests.HTTPError):
            backend.fetch("missing")

    @responses.activate
    def test_custom_headers_are_forwarded(self) -> None:
        backend = HttpBackend(base_url=BASE_URL, headers={"Authorization": "Bearer tok"})
        responses.add(responses.GET, f"{BASE_URL}/pipelines/p1", json={"id": "p1", "last_run": None, "last_error": None}, status=200)
        backend.fetch("p1")
        assert responses.calls[0].request.headers["Authorization"] == "Bearer tok"
