"""Subset of :mod:`distutils.util` containing :func:`strtobool` only."""
from __future__ import annotations

__all__ = ["strtobool"]

_TRUE_SET = {"y", "yes", "t", "true", "on", "1"}
_FALSE_SET = {"n", "no", "f", "false", "off", "0"}


def strtobool(val: str) -> int:
    """Return 1 for truthy strings and 0 for falsy ones.

    Replicates the behaviour of :func:`distutils.util.strtobool` that dbt
    depends on, raising :class:`ValueError` for unrecognised values.
    """

    lowered = val.lower()
    if lowered in _TRUE_SET:
        return 1
    if lowered in _FALSE_SET:
        return 0
    raise ValueError(f"invalid truth value {val!r}")

