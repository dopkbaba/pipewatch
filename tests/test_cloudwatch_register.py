"""Tests for the CloudWatch backend registration."""
from __future__ import annotations

from pipewatch.backends import get_backend, available_backends
from pipewatch.backends.cloudwatch import CloudWatchBackend
import pipewatch.backends.cloudwatch_register  # noqa: F401 – trigger registration


def test_cloudwatch_backend_is_registered():
    assert "cloudwatch" in available_backends()


def test_factory_creates_cloudwatch_backend_with_defaults():
    backend = get_backend("cloudwatch", {})
    assert isinstance(backend, CloudWatchBackend)
    assert backend.namespace == "PipeWatch"
    assert backend.region == "us-east-1"


def test_factory_passes_custom_config():
    backend = get_backend("cloudwatch", {"namespace": "MyNS", "region": "eu-west-1"})
    assert isinstance(backend, CloudWatchBackend)
    assert backend.namespace == "MyNS"
    assert backend.region == "eu-west-1"
