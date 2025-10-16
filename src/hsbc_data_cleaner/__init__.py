"""HSBC fund PDF cleaning toolkit."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("hsbc-data-cleaner")
except PackageNotFoundError:  # pragma: no cover - package not installed
    __version__ = "0.0.0"

__all__ = ["__version__"]
