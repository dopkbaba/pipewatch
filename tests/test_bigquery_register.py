"""Tests for BigQuery backend registration."""
from __future__ import annotations

import pytest

from pipewatch.backends import available_backends, get_backend

import pipewatch.backends.bigquery_register  # noqa: F401  side-effect import


def test_bigquery_backend_is_registered():
    assert "bigquery" in available_backends()


def test_factory_creates_bigquery_backend_with_defaults():
    from pipewatch.backends.bigquery import BigQueryBackend

    backend = get_backend("bigquery", {"project": "my_proj", "dataset": "my_ds"})
    assert isinstance(backend, BigQueryBackend)
    assert backend.project == "my_proj"
    assert backend.dataset == "my_ds"
    assert backend.table == "pipeline_metrics"


def test_factory_passes_custom_config():
    from pipewatch.backends.bigquery import BigQueryBackend

    backend = get_backend(
        "bigquery",
        {"project": "p", "dataset": "d", "table": "custom_table"},
    )
    assert isinstance(backend, BigQueryBackend)
    assert backend.table == "custom_table"
