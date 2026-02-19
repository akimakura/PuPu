"""
Бизнес метрики.
"""

from prometheus_client import Counter

MODELS = Counter(
    "model_requests_total",
    "Total count of requests by model_name",
    ["model_name"],
)
