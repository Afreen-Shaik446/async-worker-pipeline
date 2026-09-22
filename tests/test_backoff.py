"""Unit tests for backoff math. No broker or MongoDB required."""
import pytest

from worker import config
from worker.consumer import compute_backoff_seconds


def test_backoff_doubles_each_attempt(monkeypatch):
    monkeypatch.setattr(config, "BACKOFF_BASE_SECONDS", 2.0)
    monkeypatch.setattr(config, "BACKOFF_CAP_SECONDS", 10_000.0)
    assert compute_backoff_seconds(0) == 2.0
    assert compute_backoff_seconds(1) == 4.0
    assert compute_backoff_seconds(2) == 8.0
    assert compute_backoff_seconds(3) == 16.0


def test_backoff_is_capped(monkeypatch):
    monkeypatch.setattr(config, "BACKOFF_BASE_SECONDS", 2.0)
    monkeypatch.setattr(config, "BACKOFF_CAP_SECONDS", 30.0)
    assert compute_backoff_seconds(10) == 30.0


def test_backoff_never_negative_or_zero(monkeypatch):
    monkeypatch.setattr(config, "BACKOFF_BASE_SECONDS", 2.0)
    monkeypatch.setattr(config, "BACKOFF_CAP_SECONDS", 300.0)
    for attempt in range(8):
        assert compute_backoff_seconds(attempt) > 0
