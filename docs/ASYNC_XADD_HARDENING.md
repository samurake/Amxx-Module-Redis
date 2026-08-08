# Async XADD hardening

This build treats `redis_async_xadd*()` as a durable, bounded transport
boundary. It does not rely on every Pawn caller being well behaved.

## Enforced limits

- Stream key: 1 to 191 bytes.
- Event id: 1 to 191 bytes.
- Payload: 1 to 8,191 bytes. Oversized JSON is rejected, never truncated.
- Queue: 4,096 commands and 2 MiB per connection by default. The count and
  byte totals include the command currently executing on the worker.
- Configurable queue count: 1 to 16,384 commands.
- Connections: 16.
- Pending duplicate connect requests for the default handle: 1,024.
- Result and connection-result queues: 16,384 entries each.
- XADD admission: 256 submissions per connection per one-second window.
- Worker error strings returned to Pawn: 1,024 bytes.

XADD lengths are measured directly in Pawn memory with a bounded scan before
constructing C++ strings. The scan stops at the relevant maximum plus one cell,
so an oversized payload is rejected without copying or traversing the rest of a
maliciously large Pawn buffer.

`redis_async_queue_bytes*()` exposes current per-handle command memory
accounting, and `redis_async_dropped_results()` exposes cumulative callback
drops caused by full bounded result queues.

Release builds enable stack-protector and fortified libc checks. Linux artifacts
also use non-executable stack and full RELRO/BIND_NOW linker hardening; Windows
artifacts request SDL checks, ASLR, and DEP compatibility.

Positive XADD request ids are reserved until the result is dispatched on the
AMXX main thread. A second pending XADD with the same connection and request id
returns `-1`. Request id `0` is retained for legacy uncorrelated callers, but
must not be used by a durable producer that releases local state from a
callback.

## Durability boundary

`redis_async_xadd*()` returning `0` only confirms admission to the module's
bounded, volatile memory queue. It is not a Redis acknowledgement and is lost
if HLDS terminates before the worker completes the command.

A validated success callback confirms that the Redis primary accepted XADD. It
does not by itself guarantee that bytes reached persistent storage or a
replica. That depends on the host's Redis AOF/RDB and replication configuration.
The host must explicitly approve `appendonly` and `appendfsync` policy for the
acceptable loss window. This module does not issue `WAIT`, and reconnect retries
can create more than one stream entry for the same `event_id` after an ambiguous
network failure. Consumers must deduplicate by `event_id`.

The module does not issue `BGSAVE` during plugin/module unload. Map transitions
must not unexpectedly fork Redis or override the persistence schedule chosen by
the host.

## Producer contract

A durable Pawn producer must:

1. Keep the local record until a validated success callback.
2. Validate stream, event id, and payload lengths before calling the native.
3. Capture the enqueue return value and back off on `-1`.
4. Use a positive request id mapped to the pending local context.
5. Validate connection id, request id, command, stream key, event id, status,
   and Redis stream id in `Redis_Async_OnResultEx`.
6. Time out a missing callback and retry the same idempotent event id.
7. Pace retries and batch submission rather than looping on queue failure.

## Host validation checklist

Do not replace the live module until all checks pass on an isolated server:

1. Confirm the artifact is a 32-bit i386 ELF shared object accepted by the
   host's file scanner.
2. Start HLDS and verify `amxx modules` reports the Redis module as running.
   Confirm it reports version `0.3.2-streams-hardened`.
3. Record the Redis persistence policy (`appendonly`, `appendfsync`, RDB
   schedule, replication) and have the service owner approve the resulting
   recovery-point objective before calling the stream durable.
4. Compile and load `redis_xadd_validation_test.sma`, configure its protected
   connection cvars, and run `redis_xadd_validate` from the server console. If
   `redis_xadd_validation_host` is left empty, the validation plugin reuses the
   already-protected `sar_redis_host`, `sar_redis_port`,
   `sar_redis_username`, and `sar_redis_password` profile atomically. Set a
   dedicated validation host to use the isolated override profile instead.
5. Require its final summary to report `result=PASS`. It automatically verifies
   empty and oversized input rejection, exact maximum-size acceptance, duplicate
   pending request rejection, callback correlation, stream ID syntax, queue
   accounting returning to zero, absence of callback drops, and the
   256-per-second admission limit.
   `redis_xadd_status` exposes the latest result and callback counts
   to the protected server console so asynchronous completion can be checked
   without relying on log-tail timing.
6. Keep the server running for at least one additional map change and confirm
   the module unload/reload path does not hang or crash.
7. Stop Redis, generate traffic, and verify queue count/bytes remain bounded
   and Pawn receives queue failures instead of blocking the game thread.
8. Restore Redis and verify ordered recovery plus one correctly correlated
   callback per accepted XADD.
9. After the automated rate test, wait one second and confirm a normal XADD is
   accepted again.
10. Confirm Redis AOF growth, rewrite latency, stream pending count, and host
   memory remain within operational thresholds during a sustained test.

Keep Rank Transport v3 in legacy mode until these checks pass. A module change
requires a full HLDS process restart; a map change is not sufficient.
