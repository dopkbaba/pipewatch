"""Register the gRPC backend under the name ``grpc``."""
from __future__ import annotations

from typing import Any, Dict

from pipewatch.backends import register_backend


def _factory(config: Dict[str, Any]):
    """Instantiate :class:`GrpcBackend` from a config dict.

    Expected keys:
      - ``host``    (str, default ``"localhost"``)
      - ``port``    (int, default ``50051``)
      - ``timeout`` (float, default ``5.0``)
    """
    import grpc  # type: ignore
    from pipewatch.backends.grpc import GrpcBackend

    # Lazy import of the generated stub — users supply their own proto.
    # For a real deployment replace the import below with the generated module.
    try:
        from pipewatch_proto import pipeline_pb2_grpc as pb2_grpc  # type: ignore
    except ImportError as exc:  # pragma: no cover
        raise ImportError(
            "pipewatch_proto package not found. "
            "Generate gRPC stubs from the pipewatch .proto file first."
        ) from exc

    host = config.get("host", "localhost")
    port = int(config.get("port", 50051))
    timeout = float(config.get("timeout", 5.0))

    channel = grpc.insecure_channel(f"{host}:{port}")
    stub = pb2_grpc.PipelineServiceStub(channel)
    return GrpcBackend(stub=stub, timeout=timeout)


register_backend("grpc", _factory)
