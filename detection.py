"""Composable anomaly detectors for NAB operational time-series records.

This module is intentionally standalone: it does not change the existing
Kafka/Spark ingestion pipeline. Each detector accepts an iterable of records
with ``timestamp`` and ``value`` fields and returns scored dictionaries.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from statistics import mean, pstdev
from typing import Any, Iterable, Mapping


@dataclass(frozen=True)
class DetectionConfig:
    threshold: float = 3.0
    rolling_window: int = 100
    min_periods: int = 20

    def __post_init__(self) -> None:
        if self.threshold <= 0:
            raise ValueError("threshold must be positive")
        if self.rolling_window < 2:
            raise ValueError("rolling_window must be at least 2")
        if not 1 <= self.min_periods <= self.rolling_window:
            raise ValueError("min_periods must be between 1 and rolling_window")


def _numeric_record(record: Mapping[str, Any]) -> tuple[str, float]:
    if "timestamp" not in record or "value" not in record:
        raise ValueError("each record must contain timestamp and value")
    timestamp = str(record["timestamp"])
    value = float(record["value"])
    return timestamp, value


def static_zscore(
    records: Iterable[Mapping[str, Any]],
    *,
    baseline_mean: float,
    baseline_stddev: float,
    threshold: float = 3.0,
) -> list[dict[str, Any]]:
    """Score against a separately calibrated reference population."""
    if baseline_stddev <= 0:
        raise ValueError("baseline_stddev must be positive")
    if threshold <= 0:
        raise ValueError("threshold must be positive")

    results = []
    for record in records:
        timestamp, value = _numeric_record(record)
        zscore = (value - baseline_mean) / baseline_stddev
        results.append({
            **record,
            "static_zscore": zscore,
            "static_anomaly": abs(zscore) >= threshold,
        })
    return results


def rolling_zscore(
    records: Iterable[Mapping[str, Any]],
    *,
    config: DetectionConfig = DetectionConfig(),
) -> list[dict[str, Any]]:
    """Score each point against the preceding N values, then update history.

    This ordering prevents a candidate anomaly from moving its own baseline.
    Pass records in event-time order for deterministic results.
    """
    history: deque[float] = deque(maxlen=config.rolling_window)
    results = []
    for record in records:
        timestamp, value = _numeric_record(record)
        n = len(history)
        baseline_mean = mean(history) if n else None
        baseline_stddev = pstdev(history) if n > 1 else None
        if n >= config.min_periods and baseline_stddev and baseline_stddev > 0:
            zscore = (value - baseline_mean) / baseline_stddev
            anomaly = abs(zscore) >= config.threshold
        else:
            zscore = None
            anomaly = None

        results.append({
            **record,
            "rolling_mean": baseline_mean,
            "rolling_stddev": baseline_stddev,
            "rolling_sample_count": n,
            "rolling_zscore": zscore,
            "rolling_anomaly": anomaly,
        })
        history.append(value)
    return results


def detect_anomalies(
    records: Iterable[Mapping[str, Any]],
    *,
    config: DetectionConfig = DetectionConfig(),
    static_baseline: tuple[float, float] | None = None,
) -> list[dict[str, Any]]:
    """Apply rolling detection and optional static detection to records."""
    materialized = list(records)
    scored = rolling_zscore(materialized, config=config)
    if static_baseline is not None:
        scored = static_zscore(
            scored,
            baseline_mean=static_baseline[0],
            baseline_stddev=static_baseline[1],
            threshold=config.threshold,
        )
    for row in scored:
        flags = [row.get("rolling_anomaly"), row.get("static_anomaly")]
        known_flags = [flag for flag in flags if flag is not None]
        row["is_anomaly"] = any(known_flags) if known_flags else None
    return scored


def summarize_detection(
    scored_records: Iterable[Mapping[str, Any]],
    anomaly_timestamps: Iterable[str],
) -> dict[str, int | float]:
    """Simple point-level summary against labeled anomalous timestamps."""
    truth = set(map(str, anomaly_timestamps))
    tp = fp = fn = 0
    for record in scored_records:
        timestamp = str(record["timestamp"])
        predicted = bool(record.get("is_anomaly"))
        actual = timestamp in truth
        tp += int(predicted and actual)
        fp += int(predicted and not actual)
        fn += int(actual and not predicted)
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    return {"true_positive": tp, "false_positive": fp, "false_negative": fn,
            "precision": precision, "recall": recall}
