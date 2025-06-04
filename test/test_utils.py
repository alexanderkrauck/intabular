"""Tests for utility functions"""

import pytest
from unittest.mock import patch

from intabular.core.utils import parallel_map


@pytest.mark.no_llm
def test_parallel_map_retry_success():
    """Function should retry once and succeed"""
    call_counts = {}

    def flaky(x):
        count = call_counts.get(x, 0)
        call_counts[x] = count + 1
        if count == 0:
            raise ValueError("fail")
        return x * 2

    # Patch sleep to avoid slowdown from retry backoff
    with patch("intabular.core.utils.time.sleep", return_value=None):
        result = parallel_map(flaky, [1, 2, 3], retries=1)

    assert result == [2, 4, 6]
    assert all(count == 2 for count in call_counts.values())
