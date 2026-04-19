"""Tests for kinesis_register."""
from __future__ import annotations

from unittest.mock import patch, MagicMock

import pytest


def test_kinesis_backend_is_registered():
    import pipewatch.backends.kinesis_register  # noqa: F401
    from pipewatch.backends import available_backends
    assert "kinesis" in available_backends()


def test_factory_creates_kinesis_backend_with_defaults():
    import pipewatch.backends.kinesis_register  # noqa: F401
    from pipewatch.backends import get_backend

    config = {"stream_name": "my-stream"}
    with patch("pipewatch.backends.kinesis.KinesisBackend.__init__", return_value=None) as mock_init:
        get_backend("kinesis", config)
        mock_init.assert_called_once_with(
            stream_name="my-stream",
            region_name="us-east-1",
            shard_iterator_type="LATEST",
            endpoint_url=None,
        )


def test_factory_passes_custom_config():
    import pipewatch.backends.kinesis_register  # noqa: F401
    from pipewatch.backends import get_backend

    config = {
        "stream_name": "prod-stream",
        "region_name": "eu-west-1",
        "shard_iterator_type": "TRIM_HORIZON",
        "endpoint_url": "http://localhost:4566",
    }
    with patch("pipewatch.backends.kinesis.KinesisBackend.__init__", return_value=None) as mock_init:
        get_backend("kinesis", config)
        mock_init.assert_called_once_with(
            stream_name="prod-stream",
            region_name="eu-west-1",
            shard_iterator_type="TRIM_HORIZON",
            endpoint_url="http://localhost:4566",
        )
