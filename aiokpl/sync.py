"""Synchronous façade over :class:`aiokpl.producer.Producer`.

Phase 8 — the bridge for callers that **don't** run an async event loop:
scripts, Flask/Django request handlers, Jupyter cells, Celery tasks. The
async :class:`Producer` is wrapped by a :class:`SyncProducer` that owns a
private :class:`anyio.from_thread.BlockingPortal`. The portal runs an
``anyio`` event loop on a single background thread; every sync call dispatches
into it via :meth:`BlockingPortal.call`.

Why a portal instead of a raw ``threading.Thread`` + ``asyncio.run``?
``anyio.from_thread.start_blocking_portal`` already does exactly what we'd
hand-roll — pick a backend (``asyncio`` or ``trio``), start the loop on a
worker thread, expose a thread-safe :meth:`call` that schedules a coroutine
back onto that loop, and tear it down cleanly on exit. Reinventing it would
duplicate logic anyio has already debugged across both backends.

Why a single long-lived dispatcher task instead of one ``portal.call`` per
operation? :class:`anyio.abc.TaskGroup` and :class:`anyio.CancelScope` bind
to the task that *opened* them. ``aiokpl.producer.Producer`` lazily creates
per-stream pipelines (each with its own TaskGroup) on the first
``put_record``; if those creations happened inside whichever ad-hoc task
``portal.call`` spawned for that operation, the stages couldn't be cleanly
exited from the producer's owning task. We solve this by running a single
persistent task on the portal that owns the Producer's lifecycle and
consumes commands from a memory-object stream — every operation on the
async Producer happens in that one task, so cancel scopes stay coherent.

The public surface deliberately keeps anyio types out of sight: callers see a
:class:`SyncProducer` and an opaque :class:`SyncOutcome` they wait on. No
``await``, no ``asyncio.run``, no ``anyio.run`` anywhere in the consumer's
code.
"""

from __future__ import annotations

import contextlib
import threading
from typing import Any, Generic, TypeVar

import anyio
from anyio.from_thread import BlockingPortal, start_blocking_portal

from aiokpl.aggregation import Tag
from aiokpl.config import Config
from aiokpl.outcome import Outcome
from aiokpl.producer import Producer
from aiokpl.result import RecordResult

T = TypeVar("T")

_DEFAULT_EXIT_FLUSH_TIMEOUT_S = 30.0
_FLUSH_POLL_INTERVAL_S = 0.01


class SyncOutcome(Generic[T]):
    """Synchronous handle to an async :class:`Outcome` running on the portal.

    Never instantiated by user code — :meth:`SyncProducer.put_record` returns
    one. The wrapped :class:`Outcome` lives on the portal's event loop; every
    operation here is a :meth:`BlockingPortal.call` round-trip so reads of the
    underlying anyio primitives happen on the right thread.
    """

    __slots__ = ("_outcome", "_portal")

    def __init__(self, portal: BlockingPortal, outcome: Outcome[T]) -> None:
        self._portal = portal
        self._outcome = outcome

    def wait(self, timeout: float | None = None) -> T:
        """Block until the outcome resolves or ``timeout`` (seconds) elapses.

        Raises :class:`TimeoutError` on timeout. Any exception the underlying
        :class:`Outcome` was set with propagates verbatim.
        """
        try:
            return self._portal.call(_wait_with_timeout, self._outcome, timeout)
        except TimeoutError as exc:
            # anyio raises a builtins.TimeoutError on fail_after expiry; the
            # explicit re-raise documents intent and keeps the type the
            # docstring promises even if anyio's subclassing ever changes.
            raise TimeoutError(f"SyncOutcome.wait timed out after {timeout}s") from exc

    def done(self) -> bool:
        """``True`` once the underlying outcome has been set."""
        return self._portal.call(_is_set, self._outcome)

    def cancel(self) -> bool:
        """Best-effort cancellation.

        Sets the underlying outcome to :class:`asyncio.CancelledError` if it
        has not already been set. Returns ``True`` if this call performed the
        cancellation, ``False`` if the outcome was already resolved. The
        in-flight Kinesis request is **not** stopped — this only resolves the
        local handle so callers waiting on it unblock with an exception.
        """
        return self._portal.call(_cancel, self._outcome)


async def _wait_with_timeout(outcome: Outcome[T], deadline: float | None) -> T:
    # ASYNC109 frowns on async ``timeout`` params; we explicitly route the
    # value through ``anyio.fail_after`` so the rename is just cosmetic.
    if deadline is None:
        return await outcome.wait()
    with anyio.fail_after(deadline):
        return await outcome.wait()


async def _is_set(outcome: Outcome[Any]) -> bool:
    return outcome.is_set()


class SyncOutcomeCancelled(Exception):
    """Raised by :meth:`SyncOutcome.wait` after :meth:`SyncOutcome.cancel`.

    Distinct from :class:`asyncio.CancelledError` because that one is treated
    specially by ``concurrent.futures`` (it's interpreted as future
    cancellation by the portal's machinery) and would surface as
    :class:`concurrent.futures.CancelledError` on the sync side rather than
    propagating through the user's normal exception handling.
    """


async def _cancel(outcome: Outcome[Any]) -> bool:
    if outcome.is_set():
        return False
    outcome.set_exception(SyncOutcomeCancelled("SyncOutcome.cancel"))
    return True


class _Command:
    """One command for the dispatcher task to execute against the Producer.

    A :class:`threading.Event` signals completion to the sync caller; the
    dispatcher fills :attr:`result` or :attr:`exc` before setting it. This is
    a simpler/safer cross-thread handoff than :class:`concurrent.futures` —
    we don't need cancellation across the boundary.
    """

    __slots__ = ("done", "exc", "kind", "kwargs", "result")

    def __init__(self, kind: str, **kwargs: Any) -> None:
        self.kind = kind
        self.kwargs = kwargs
        self.done = threading.Event()
        self.result: Any = None
        self.exc: BaseException | None = None


class SyncProducer:
    """Synchronous façade over :class:`aiokpl.producer.Producer`.

    Owns a :class:`BlockingPortal` running an ``anyio`` event loop on a
    background thread, plus a single dispatcher task on that loop that owns
    the underlying async :class:`Producer`. Operations dispatch via a memory
    object stream so every action on the Producer runs in the same task — a
    requirement of :class:`anyio.abc.TaskGroup`'s cancel-scope binding.

    Lifecycle is a regular ``with`` block::

        with SyncProducer(config) as producer:
            outcome = producer.put_record(
                stream="my-stream",
                partition_key="user-123",
                data=b"hello",
            )
            result = outcome.wait(timeout=5.0)

    :meth:`put_record` is thread-safe: each call enqueues one command onto
    the portal's stream, the dispatcher serializes them. Concurrent callers
    from many OS threads simply queue up.

    ``backend`` is forwarded to
    :func:`anyio.from_thread.start_blocking_portal`. Default ``"asyncio"``
    because ``aiobotocore`` (the async Producer's HTTP client) is
    asyncio-only — passing ``"trio"`` is accepted by the constructor but
    will fail at ``__enter__`` when the producer tries to import its
    aiobotocore session.
    """

    __slots__ = (
        "_backend",
        "_command_send",
        "_config",
        "_dispatcher_started",
        "_dispatcher_stopped",
        "_entered",
        "_portal",
        "_portal_cm",
        "_producer_ref",
    )

    def __init__(self, config: Config, *, backend: str = "asyncio") -> None:
        self._config = config
        self._backend = backend
        self._portal_cm: Any = None
        self._portal: BlockingPortal | None = None
        self._command_send: Any = None
        self._dispatcher_started: threading.Event = threading.Event()
        self._dispatcher_stopped: threading.Event = threading.Event()
        self._producer_ref: Producer | None = None
        self._entered = False

    # ─── Lifecycle ─────────────────────────────────────────────────────────

    def __enter__(self) -> SyncProducer:
        self._portal_cm = start_blocking_portal(backend=self._backend)
        self._portal = self._portal_cm.__enter__()
        startup_error: list[BaseException | None] = [None]
        try:
            # Start the dispatcher and wait for it to either come up or fail.
            self._portal.start_task_soon(self._run_dispatcher, startup_error)
            self._dispatcher_started.wait()
            if startup_error[0] is not None:
                raise startup_error[0]
        except BaseException:
            # Make sure the portal goes down cleanly even if the dispatcher
            # never finished entering the Producer.
            self._command_send = None
            self._portal_cm.__exit__(None, None, None)
            self._portal = None
            self._portal_cm = None
            raise
        self._entered = True
        return self

    def __exit__(self, *exc: Any) -> None:
        self._entered = False
        portal_cm = self._portal_cm
        try:
            # Don't escalate flush timeouts past __exit__: the user is already
            # on the close path, and the dispatcher's shutdown path below
            # still drives the async Producer's __aexit__ which drains
            # whatever is in flight.
            with contextlib.suppress(TimeoutError):
                self._dispatch("flush", timeout=_DEFAULT_EXIT_FLUSH_TIMEOUT_S)
            # Tell the dispatcher to exit; the Producer's __aexit__ runs
            # inside it.
            self._dispatch("shutdown")
            self._dispatcher_stopped.wait(timeout=60.0)
        finally:
            self._command_send = None
            portal_cm.__exit__(None, None, None)
            self._portal = None
            self._portal_cm = None

    async def _run_dispatcher(self, startup_error: list[BaseException | None]) -> None:
        """Long-running task that owns the async Producer and serializes ops.

        Reads commands from a memory object stream and dispatches them to the
        underlying Producer. Stays alive until a ``shutdown`` command arrives;
        then exits the Producer (inside this same task) and returns.
        """
        send, receive = anyio.create_memory_object_stream[_Command](max_buffer_size=0)
        self._command_send = send
        producer: Producer | None = None
        try:
            async with send, receive:
                candidate = Producer(self._config)
                try:
                    await candidate.__aenter__()
                except BaseException as exc:
                    startup_error[0] = exc
                    self._dispatcher_started.set()
                    return
                producer = candidate
                self._producer_ref = producer
                self._dispatcher_started.set()
                while True:
                    cmd = await receive.receive()
                    if cmd.kind == "shutdown":
                        cmd.done.set()
                        break
                    try:
                        cmd.result = await self._execute(producer, cmd)
                    except BaseException as exc:
                        cmd.exc = exc
                    finally:
                        cmd.done.set()
        finally:
            if producer is not None:
                # The sync user already received their results; swallowing
                # exit errors here matches what we do in the sync __exit__
                # for the bracketed flush.
                with contextlib.suppress(BaseException):
                    await producer.__aexit__(None, None, None)
            self._producer_ref = None
            self._dispatcher_stopped.set()

    async def _execute(self, producer: Producer, cmd: _Command) -> Any:
        if cmd.kind == "put_record":
            return await producer.put_record(**cmd.kwargs)
        # The only other kind that reaches _execute is ``flush`` — the
        # ``shutdown`` kind is intercepted by the dispatcher loop above.
        timeout = cmd.kwargs.get("timeout")
        # ASYNC110 wants an ``anyio.Event``; we have none — the underlying
        # Producer exposes only a counter incremented/decremented from inside
        # the pipeline. Polling is the pragmatic bridge.
        if timeout is None:
            await producer.flush()
            while producer.outstanding_records > 0:  # noqa: ASYNC110
                await anyio.sleep(_FLUSH_POLL_INTERVAL_S)
        else:
            with anyio.fail_after(timeout):
                await producer.flush()
                while producer.outstanding_records > 0:  # noqa: ASYNC110
                    await anyio.sleep(_FLUSH_POLL_INTERVAL_S)
        return None

    def _dispatch(self, kind: str, **kwargs: Any) -> Any:
        # Callers gate on ``self._entered``; by the time we get here both the
        # portal and the command send-stream are live.
        send = self._command_send
        portal = self._portal
        assert send is not None
        assert portal is not None
        cmd = _Command(kind, **kwargs)
        # ``send.send`` is async; the portal dispatches it onto the loop. The
        # dispatcher task is suspended in ``receive`` and will be woken by the
        # send completing.
        portal.call(send.send, cmd)
        cmd.done.wait()
        if cmd.exc is not None:
            raise cmd.exc
        return cmd.result

    # ─── Public API ────────────────────────────────────────────────────────

    @property
    def outstanding_records(self) -> int:
        """Records currently in flight on the wrapped async Producer.

        Read directly off the Producer instance; safe from any thread because
        it's a single-int load (atomic in CPython) and the Producer's own
        counter is mutated only under its own event loop.
        """
        producer = self._producer_ref
        if producer is None:
            return 0
        return producer.outstanding_records

    def put_record(
        self,
        *,
        stream: str,
        partition_key: str,
        data: bytes,
        explicit_hash_key: str | None = None,
        tags: tuple[Tag, ...] = (),
    ) -> SyncOutcome[RecordResult]:
        """Submit a record. Returns a :class:`SyncOutcome` immediately.

        Blocks only long enough to round-trip the put through the dispatcher
        and register its :class:`Outcome`; the Kinesis call happens in the
        background. The returned :class:`SyncOutcome` resolves with the
        terminal :class:`RecordResult` once the Retrier classifies the
        record.

        Thread-safe — call from any OS thread.
        """
        if not self._entered:
            raise RuntimeError("SyncProducer not entered")
        outcome = self._dispatch(
            "put_record",
            stream=stream,
            partition_key=partition_key,
            data=data,
            explicit_hash_key=explicit_hash_key,
            tags=tags,
        )
        portal = self._portal
        assert portal is not None
        return SyncOutcome(portal, outcome)

    def flush(self, *, timeout: float | None = None) -> None:
        """Block until every in-flight record reaches a terminal state.

        Raises :class:`TimeoutError` if ``timeout`` (seconds) elapses before
        the queue drains. ``timeout=None`` waits forever.
        """
        if not self._entered:
            raise RuntimeError("SyncProducer not entered")
        try:
            self._dispatch("flush", timeout=timeout)
        except TimeoutError as exc:
            raise TimeoutError(f"SyncProducer.flush timed out after {timeout}s") from exc


__all__ = ["SyncOutcome", "SyncOutcomeCancelled", "SyncProducer"]
