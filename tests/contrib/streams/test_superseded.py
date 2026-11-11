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


def _workflow(run: str) -> models.Producer:
    return models.Producer(
        id=f"wf:{run}",
        kind=models.ProducerKindWorkflow(
            models.WorkflowProducer(
                run_id=run, epoch=models.WorkflowTaskEpoch(attempt=1)
            )
        ),
    )


def _local_activity(id: str, run: str, attempt: int) -> models.Producer:
    return models.Producer(
        id=id,
        kind=models.ProducerKindLocalActivity(
            models.LocalActivityProducer(
                run_id=run,
                epoch=models.LocalActivityEpoch(
                    task=models.WorkflowTaskEpoch(attempt=attempt), attempt=attempt
                ),
            )
        ),
    )


def _client(attempt: int) -> models.Producer:
    return models.Producer(
        id="client:me",
        kind=models.ProducerKindClient(
            models.ClientProducer(epoch=models.ClientEpoch(attempt=attempt))
        ),
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


async def test_a_client_producer_never_supersedes():
    # A newer client epoch only fences, so a client's Records are never withdrawn.
    items = await _wrapped(_record(10, _client(1)), _record(20, _client(2)))
    assert not any(isinstance(i, Superseded) for i in items)
    # withdraws() also never matches a client's Records.
    synthetic = Superseded(
        offset=StreamOffset._from_wire(_off(20)),
        producer=_activity("act:x", "r", attempt=2, seconds=101),
    )
    assert not synthetic.withdraws(_record(10, _client(1)))


async def test_a_workflow_producer_never_supersedes():
    # A run's Producer id is stable across its Workflow Tasks, so a failed task's Records stay
    # visible.
    run = _workflow("run-1")
    items = await _wrapped(_record(10, run), _record(20, run))
    assert not any(isinstance(i, Superseded) for i in items)


async def test_a_local_activity_producer_never_supersedes():
    first = _local_activity("la:run-1:k", "run-1", attempt=1)
    retry = _local_activity("la:run-1:k", "run-1", attempt=2)
    items = await _wrapped(_record(10, first), _record(20, retry))
    assert not any(isinstance(i, Superseded) for i in items)


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
