"""Official Python client for StreamFlow Pulse.

Quick start:

    >>> from pulse_client import PulseClient
    >>> client = PulseClient("http://localhost:9090")
    >>> client.auth.login("alice", "secret")
    >>> for pipeline in client.pipelines.list():
    ...     print(pipeline["name"])
    >>> client.close()

Or as a context manager:

    >>> with PulseClient("http://localhost:9090", token="ey...") as client:
    ...     print(client.version())

See README.md for the full surface.
"""

from pulse_client.client import PulseClient
from pulse_client.exceptions import (
    PulseAPIError,
    PulseAuthError,
    PulseClientError,
    PulseNotFoundError,
    PulseRateLimitError,
    PulseValidationError,
)

__version__ = "2.5.8"

__all__ = [
    "PulseClient",
    "PulseAPIError",
    "PulseAuthError",
    "PulseClientError",
    "PulseNotFoundError",
    "PulseRateLimitError",
    "PulseValidationError",
    "__version__",
]
