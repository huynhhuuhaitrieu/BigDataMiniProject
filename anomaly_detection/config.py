from dataclasses import asdict, dataclass
import hashlib
import json
import math
from functools import cached_property


@dataclass(frozen=True)
class DetectorConfig:
    history_size: int = 100
    min_periods: int = 20
    threshold: float = 3.0
    epsilon: float = 1e-9
    absolute_tolerance: float = 0.01
    mode: str = 'ordered'
    lateness_seconds: int = 600
    max_pending: int = 10000

    def __post_init__(self):
        if not 2 <= self.history_size or not 2 <= self.min_periods <= self.history_size:
            raise ValueError('Require 2 <= min_periods <= history_size')
        for key in ('threshold', 'epsilon', 'absolute_tolerance'):
            if not math.isfinite(getattr(self, key)) or getattr(self, key) <= 0:
                raise ValueError(f'{key} must be finite and positive')
        if self.mode not in ('ordered', 'buffered'):
            raise ValueError('mode must be ordered or buffered')
        if self.lateness_seconds <= 0 or self.max_pending < 1:
            raise ValueError('lateness_seconds and max_pending must be positive')

    @cached_property
    def version(self):
        return hashlib.sha256(json.dumps(asdict(self), sort_keys=True).encode()).hexdigest()[:16]
