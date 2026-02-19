from unittest.mock import Mock

import pytest
from py_common_lib.metrics import metrics


@pytest.fixture
def mock_metrics(monkeypatch):
    mock = Mock()
    monkeypatch.setattr(metrics, "HEALTH_CHECKS", mock)
    return mock
