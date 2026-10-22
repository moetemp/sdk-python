from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

import pytest

from temporalio.common import RetryPolicy
from temporalio.contrib.streams.redis import chain_continues
from temporalio.exceptions import ApplicationError

START = datetime(2026, 10, 21, tzinfo=timezone.utc)


def info(**fields: Any) -> Any:
    base: dict[str, Any] = {
        "cron_schedule": None,
        "retry_policy": None,
        "attempt": 1,
        "execution_timeout": None,
        "workflow_start_time": START,
        "execution_expiration_time": None,
    }
    return SimpleNamespace(**{**base, **fields})


RETRY = RetryPolicy(initial_interval=timedelta(seconds=1), maximum_attempts=3)


@pytest.mark.parametrize(
    "case, run, failure, expected",
    [
        ("success", info(), None, False),
        ("cron success", info(cron_schedule="* * * * *"), None, True),
        ("cron failure", info(cron_schedule="* * * * *"), RuntimeError("x"), True),
        ("no retry policy", info(), RuntimeError("x"), False),
        ("attempts left", info(retry_policy=RETRY), RuntimeError("x"), True),
        ("last attempt", info(retry_policy=RETRY, attempt=3), RuntimeError("x"), False),
        (
            "unlimited attempts",
            info(retry_policy=RetryPolicy(maximum_attempts=0), attempt=50),
            RuntimeError("x"),
            True,
        ),
        (
            "non-retryable error",
            info(retry_policy=RETRY),
            ApplicationError("x", non_retryable=True),
            False,
        ),
        (
            "non-retryable type",
            info(
                retry_policy=RetryPolicy(non_retryable_error_types=["Bad"]),
            ),
            ApplicationError("x", type="Bad"),
            False,
        ),
        (
            "no time left",
            info(retry_policy=RETRY, execution_timeout=timedelta(seconds=10)),
            RuntimeError("x"),
            False,
        ),
        (
            "no time left in a later run of the chain",
            info(
                retry_policy=RETRY,
                execution_timeout=timedelta(seconds=10),
                workflow_start_time=START + timedelta(seconds=9),
                execution_expiration_time=START + timedelta(seconds=10),
            ),
            RuntimeError("x"),
            False,
        ),
    ],
)
def test_chain_continues(case: str, run: Any, failure: Any, expected: bool):
    # "no time left" runs 9.5 s into a 10 s chain, so a 1 s backoff passes the expiration.
    now = START + timedelta(seconds=9.5)
    assert chain_continues(run, now, failure) == expected, case
