"""Tests for DynamoDB backend registration."""
from __future__ import annotations

from unittest.mock import patch

import pytest


def test_dynamodb_backend_is_registered():
    import pipewatch.backends.dynamodb_register  # noqa: F401
    from pipewatch.backends import available_backends

    assert "dynamodb" in available_backends()


def test_factory_creates_dynamodb_backend_with_defaults():
    import pipewatch.backends.dynamodb_register  # noqa: F401
    from pipewatch.backends import get_backend

    backend = get_backend("dynamodb", {"table_name": "my_table"})
    from pipewatch.backends.dynamodb import DynamoDBBackend

    assert isinstance(backend, DynamoDBBackend)
    assert backend.table_name == "my_table"
    assert backend.region_name == "us-east-1"
    assert backend.endpoint_url is None


def test_factory_passes_custom_config():
    import pipewatch.backends.dynamodb_register  # noqa: F401
    from pipewatch.backends import get_backend

    backend = get_backend("dynamodb", {
        "table_name": "prod_pipelines",
        "region_name": "eu-west-1",
        "endpoint_url": "http://localhost:8000",
    })
    assert backend.region_name == "eu-west-1"
    assert backend.endpoint_url == "http://localhost:8000"
