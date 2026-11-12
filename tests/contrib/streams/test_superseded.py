"""Unit coverage for the opt-in ``with_superseded`` library.

The base read gives ``Record | Closed`` only. ``with_superseded`` adds ``Superseded`` items,
computed from the ``Producer`` epoch that each Record carries, with no Store facts.
"""

from collections.abc import AsyncGenerator

from temporalio.streams import StreamOffset, _reader
from temporalio.streams._items import Closed, Record
from temporalio.streams._reader import Item
from temporalio.streams.superseded import Superseded, with_superseded
from temporalio.streams.system import models


def _off(n: int) -> bytes:
    return n.to_bytes(8, "big")


def _hex(n: int) -> str:
    return _off(n).hex()


def _ts(seconds: int) -> models.Timestamp:
    return models.Timestamp(seconds=seconds, nanos=0)


def _activity(
    id: str, run: str, attempt: int, seconds: int, standalone: bool = False
) -> models.Producer:
    producer = models.ActivityProducer(
        run_id=run,
        epoch=models.ActivityEpoch(
            attempt_scheduled_time=_ts(seconds), attempt=attempt
        ),
    )
    kind = (
        models.ProducerKindStandaloneActivity(producer)
        if standalone
        else models.ProducerKindWorkflowActivity(producer)
    )
    return models.Producer(id=id, kind=kind)


def _local_activity(
    id: str, run: str, *, task: int = 3, task_attempt: int = 1, attempt: int = 1
) -> models.Producer:
    return models.Producer(
        id=id,
        kind=models.ProducerKindLocalActivity(
            models.LocalActivityProducer(
                run_id=run,
                epoch=models.LocalActivityEpoch(
                    task=models.WorkflowTaskEpoch(
                        started_event_id=task, attempt=task_attempt
                    ),
                    attempt=attempt,
                ),
            )
        ),
    )


def _client(id: str = "client:me") -> models.Producer:
    return models.Producer(
        id=id, kind=models.ProducerKindClient(models.ClientProducer())
    )


def _record(offset: int, producer: models.Producer) -> Record[None]:
    return Record(
        data=None, offset=StreamOffset._from_wire(_off(offset)), producer=producer
    )


async def _wrapped(*items: Item) -> list[Record[None] | Superseded | Closed]:
    async def source() -> AsyncGenerator[Item, None]:
        for item in items:
            yield item

    reader: _reader.Reader[None] = _reader.Reader(source(), _reader.Position(None))
    return [item async for item in with_superseded(reader)]


async def test_an_activity_retry_supersedes_the_failed_attempt():
    first = _activity("act:wf:a:k", "r", attempt=1, seconds=100)
    retry = _activity("act:wf:a:k", "r", attempt=2, seconds=101)
    items = await _wrapped(_record(10, first), _record(11, first), _record(20, retry))
    [superseded] = [i for i in items if isinstance(i, Superseded)]
    assert tuple(str(o) for o in superseded.withdrawn_offsets) == (_hex(10), _hex(11))
    records = [i for i in items if isinstance(i, Record)]
    assert [str(r.offset) for r in records] == [_hex(10), _hex(11), _hex(20)]
    # The Superseded item comes before the Record whose higher epoch withdrew the earlier ones.
    assert str(items[2].offset) == _hex(20) and isinstance(items[2], Superseded)
    assert superseded.withdraws(_record(10, first))
    assert not superseded.withdraws(_record(20, retry))


async def test_a_standalone_activity_retry_supersedes_the_failed_attempt():
    first = _activity("sa:a:r", "r", attempt=1, seconds=100, standalone=True)
    retry = _activity("sa:a:r", "r", attempt=2, seconds=101, standalone=True)
    items = await _wrapped(_record(10, first), _record(20, retry))
    [superseded] = [i for i in items if isinstance(i, Superseded)]
    assert tuple(str(o) for o in superseded.withdrawn_offsets) == (_hex(10),)


async def test_known_limit_a_resumed_retry_still_withdraws_the_failed_attempt():
    # The helper takes a higher epoch as a new attempt at the same work. An Activity that resumes
    # from its heartbeat details writes only the rest, and its first Record still withdraws the
    # failed attempt's Records, though nothing writes them again.
    first = _activity("act:wf:a:k", "r", attempt=1, seconds=100)
    resumed = _activity("act:wf:a:k", "r", attempt=2, seconds=101)
    items = await _wrapped(_record(10, first), _record(11, first), _record(20, resumed))
    [superseded] = [i for i in items if isinstance(i, Superseded)]
    assert tuple(str(o) for o in superseded.withdrawn_offsets) == (_hex(10), _hex(11))


async def test_a_local_activity_retry_supersedes_the_failed_attempt():
    first = _local_activity("la:run-1:k", "run-1", attempt=1)
    retry = _local_activity("la:run-1:k", "run-1", attempt=2)
    items = await _wrapped(_record(10, first), _record(20, retry))
    [superseded] = [i for i in items if isinstance(i, Superseded)]
    assert tuple(str(o) for o in superseded.withdrawn_offsets) == (_hex(10),)


async def test_a_local_activity_key_orders_the_task_before_the_attempt():
    # A later Workflow Task beats a higher attempt in an earlier task, as in the crate's order.
    earlier_task = _local_activity("la:run-1:k", "run-1", task=5, attempt=3)
    task_retry = _local_activity("la:run-1:k", "run-1", task=5, task_attempt=2)
    later_task = _local_activity("la:run-1:k", "run-1", task=9, attempt=1)
    items = await _wrapped(
        _record(10, earlier_task), _record(20, task_retry), _record(30, later_task)
    )
    first, second = [i for i in items if isinstance(i, Superseded)]
    assert tuple(str(o) for o in first.withdrawn_offsets) == (_hex(10),)
    assert tuple(str(o) for o in second.withdrawn_offsets) == (_hex(20),)


async def test_a_client_is_never_superseded():
    # A client has no epoch, so no Record has a higher epoch of its Producer.
    one, other = _client("client:host:a"), _client("client:host:b")
    items = await _wrapped(_record(10, one), _record(20, other), _record(30, one))
    assert not any(isinstance(i, Superseded) for i in items)
    assert [str(i.offset) for i in items if isinstance(i, Record)] == [
        _hex(10),
        _hex(20),
        _hex(30),
    ]
    synthetic = Superseded(offset=StreamOffset._from_wire(_off(30)), producer=one)
    assert not synthetic.withdraws(_record(10, one))


async def test_two_runs_never_supersede_each_other():
    first = _local_activity("la:run-1:k", "run-1", task=3)
    second = _local_activity("la:run-2:k", "run-2", task=9)
    items = await _wrapped(_record(10, first), _record(20, second))
    assert not any(isinstance(i, Superseded) for i in items)
    # withdraws() also never matches a Producer of another run or kind.
    synthetic = Superseded(offset=StreamOffset._from_wire(_off(20)), producer=second)
    assert not synthetic.withdraws(_record(10, first))
    assert not synthetic.withdraws(_record(10, _client()))


async def test_withdraws_checks_a_record_kept_from_an_earlier_read():
    first = _activity("act:wf:a:k", "r", attempt=1, seconds=100)
    retry = _activity("act:wf:a:k", "r", attempt=2, seconds=101)
    live = await _wrapped(_record(10, first), _record(20, retry))
    [superseded] = [i for i in live if isinstance(i, Superseded)]
    # An app that kept the failed attempt's Record from before a reload checks it, by the Record
    # or by its Producer.
    assert superseded.withdraws(_record(10, first))
    assert superseded.withdraws(first)
    assert not superseded.withdraws(_record(20, retry))
    other = _activity("act:wf:b:k", "r2", attempt=1, seconds=100)
    assert not superseded.withdraws(_record(99, other))
