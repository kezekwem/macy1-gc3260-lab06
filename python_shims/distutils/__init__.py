"""Compatibility shim for Python's removed ``distutils`` package."""
from . import util  # noqa: F401  (re-export for callers expecting distutils.util)
__all__ = ["util"]

