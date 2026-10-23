"""Reads of a Stream in Workflow code.

Each page is a call whose answer is in History, so a replay reads the same items. A Stream
plugin's outbound interceptor reads a page with ``_temporal_stream_read_page``.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, Mapping, Sequence
from datetime import datetime, timedelta
from typing import Any

import temporalio.api.streamservice.v1 as stream_pb
import temporalio.common
import temporalio.converter
import temporalio.exceptions
import temporalio.nexus.system
import temporalio.workflow
from temporalio.contrib.streams import _convert, _errors
from temporalio.contrib.streams._items import Closed, End, Record, Superseded
from temporalio.contrib.streams.system import models

Item = Record[Any] | Superseded | Closed

# The service waits at most this long for a Stream that nobody created yet, per call.
_MAX_CREATE_WAIT = timedelta(seconds=60)
# A backend that can't park a read pauses on a durable timer after a page with no Record,
# from the first pause up to the last.
_FIRST_PAUSE = timedelta(seconds=1)
_LAST_PAUSE = timedelta(seconds=60)


def reader_of(backend: str) -> Any:
    """The outbound interceptor of the Stream plugin with this backend name."""
    outbound = getattr(temporalio.workflow._Runtime.current(), "_outbound", None)
    while outbound is not None:
        if (
            getattr(outbound, "_temporal_stream_transport", False)
            and getattr(outbound, "_temporal_stream_backend_name", None) == backend
        ):
            return outbound
        outbound = getattr(outbound, "next", None)
    raise RuntimeError(f"No Stream plugin of the Worker has the backend {backend}")


async def read(
    reader: Any,
    ref: stream_pb.StreamRef,
    *,
    topics: Sequence[str] | None,
    prefixes: Sequence[str],
    types: Mapping[str, Any],
    from_offset: int | End | None,
    wait_new_records: bool,
    create_wait: timedelta | None,
) -> AsyncGenerator[Item, None]:
    """Give the items of a Stream in order, until a close, or the newest Record when
    ``wait_new_records`` is False.
    """
    request = stream_pb.ReadRecordsRequest(
        stream=ref, topics=list(topics or []), topic_prefixes=list(prefixes)
    )
    if from_offset is End.END:
        # END is only the first page's start. Each answer's next offset pins later pages.
        request.start = stream_pb.ReadStart.READ_START_END
    elif from_offset:
        request.from_offset = from_offset
    else:
        request.start = stream_pb.ReadStart.READ_START_BEGINNING
    create_until = temporalio.workflow.now() + create_wait if create_wait else None
    pause = _FIRST_PAUSE
    while True:
        if temporalio.workflow.unsafe.is_read_only():
            raise _errors.ReadOnlyContextError("Can't read a Stream in read-only code")
        page_request = stream_pb.ReadRecordsRequest()
        page_request.CopyFrom(request)
        if wait_new_records:
            page_request.wait.FromTimedelta(reader._temporal_stream_read_wait)
        left = _left(create_until)
        if left:
            page_request.create_wait.FromTimedelta(min(left, _MAX_CREATE_WAIT))
        try:
            page = await reader._temporal_stream_read_page(page_request)
        except _errors.StreamNotFoundError:
            if not _left(create_until):
                raise
            await temporalio.workflow.sleep(pause)
            pause = min(pause * 2, _LAST_PAUSE)
            continue
        for record in page.records:
            item = workflow_item(record, types)
            if item is None:
                continue
            yield item
            if isinstance(item, Closed):
                return
        if page.done:
            return
        if page.next_offset:
            request.from_offset = page.next_offset
            request.ClearField("start")
        if page.records:
            pause = _FIRST_PAUSE
            continue
        if not wait_new_records:
            return
        if not reader._temporal_stream_parks_reads:
            # The server wake comes later. Until then a quiet Stream costs one timer and one
            # read per pause.
            await temporalio.workflow.sleep(pause)
            pause = min(pause * 2, _LAST_PAUSE)


def _left(until: datetime | None) -> timedelta | None:
    if until is None:
        return None
    left = until - temporalio.workflow.now()
    return left if left > timedelta() else None


def workflow_item(
    record: stream_pb.StreamRecord, types: Mapping[str, Any]
) -> Item | None:
    """One read item, with the body decoded by the Workflow's converter and its Topic's type.

    The codec already ran outside the sandbox, on the page.
    """
    converter = temporalio.workflow.payload_converter()
    # The generated converter makes the producer and the supersession models, and the body
    # stays a Payload that the Topic's type decodes.
    with temporalio.nexus.system._user_converter_context(
        temporalio.nexus.system._SystemNexusUserConverters(
            payload_converter=_convert._RawPayloadConverter(),
            failure_converter=_failure_converter(),
        )
    ):
        model = models._StreamRecordTransferTypeConverter().from_transfer_type(
            record, models.StreamRecord
        )
    offset = model.offset or 0
    if model.kind == stream_pb.StreamRecordKind.STREAM_RECORD_KIND_DATA:
        topic = model.topic or ""
        return Record(
            topic=topic,
            data=_decode(converter, model.body, types.get(topic)),
            offset=offset,
            producer=model.producer,
        )
    if model.kind == stream_pb.StreamRecordKind.STREAM_RECORD_KIND_SUPERSEDED:
        if model.supersession is None:
            return None
        return Superseded(offset=offset, supersession=model.supersession)
    if model.kind == stream_pb.StreamRecordKind.STREAM_RECORD_KIND_CLOSE:
        return Closed(
            offset=offset,
            result=_decode(converter, model.close_result, None),
            failure=model.close_failure,
        )
    return None


def _decode(
    converter: temporalio.converter.PayloadConverter, body: Any, type_hint: Any
) -> Any:
    if not isinstance(body, temporalio.common.RawValue):
        return body
    return converter.from_payload(body.payload, type_hint)


def _failure_converter() -> temporalio.converter.FailureConverter:
    runtime = temporalio.workflow._Runtime.current()
    return getattr(
        runtime,
        "_workflow_context_failure_converter",
        temporalio.converter.DefaultFailureConverter(),
    )


def error_of(err: BaseException) -> BaseException:
    """The Stream error that a failed page carries as its cause, or the error itself.

    A page's failure is in History, so a replay raises the same error.
    """
    cause: BaseException | None = err
    while cause is not None:
        if isinstance(cause, temporalio.exceptions.ApplicationError):
            stream_error = _errors.from_application_error(cause)
            if stream_error is not None:
                return stream_error
        cause = cause.__cause__
    return err
