# Why anyio?

`aiokpl` is a **library**, not an application. Library users in the `trio`
ecosystem cannot adopt an asyncio-only library. `anyio` is the standard
portability layer between the two runtimes, so we depend on it and write
every public stage so it works on either backend.

Practical consequences:

- Locks, events, sleeps, task groups, cancel scopes are all the `anyio` ones,
  not `asyncio`'s.
- Every component that needs a background task (`ShardMap` refresh, `Reducer`
  deadline timer, `Limiter` drain loop) owns or receives an
  `anyio.abc.TaskGroup`.
- The test suite parametrizes `anyio_backend` across `["asyncio", "trio"]`
  so each async test runs once per backend.
