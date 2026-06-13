# AMX MOD X - Redis Client Module
### Description:
> This is a module for amxmodx that allows operations from HLDS to the Redis data store.

Current fork version: `0.2.0-multi-async`.

Original author: Aoi.Kagase. Async queue maintainer: samurake.

---
### Required:
> Data store server application supporting Redis protocol.
```
Redis 
Valkey 
KeyDB 
Microsoft Garnet
DragonflyDB
```

---
### Building:
> Currently, only Linux is supported. <br>

I use ambuild for builds.

  1. Build the hiredis library that exists in public/hiredis.
  2. Build the redis++ library that exists in public/redis-plus-plus.
  3. execute `python configure.py`.
  4. execute `ambuild obj-linux/`.

> Help me set up a compiling environment for Windows.<br/>
> It also does not support automatic compilation of the external libraries hiredis and redis-plus-plus.<br/>
> I don't think I can implement these.

---
### Usage for plugins:
> This is in its infancy and there are few functions available at this time.

See ![redis.inc](redis.inc) and ![redis_test.sma](redis_test.sma)

---
### Async queue:
The original Redis natives are kept for compatibility and still execute on the
AMXX/game thread. Use `redis_async_connect()` plus the `redis_async_*` command
natives for runtime paths that must not wait on network I/O, such as analytics
publishing or write-behind snapshots.

The async API starts a worker thread and performs the Redis connection on that
worker thread. Async command natives only copy Pawn parameters into a bounded
queue and return:

```pawn
redis_async_connect("127.0.0.1", 6379, "", "secret", 100);
redis_async_set_queue_limit(8192);

redis_async_publish("amxx:server_analytics", payload);
redis_async_hset_string("amxx:server_analytics:events:2026-06-08", event_id, payload);
```

Available async write/read natives:

```pawn
redis_async_connect(const hostip[], const port = 6379, const username[] = "", const password[] = "", request_id = 0);
redis_async_open(const hostip[], const port = 6379, const username[] = "", const password[] = "", request_id = 0, const name[] = "");
redis_async_close(connection_id);
redis_async_status(connection_id);
redis_async_publish(const channel[], const message[]);
redis_async_hset_string(const key[], const field[], const value[]);
redis_async_hset_integer(const key[], const field[], const value);
redis_async_set_string(const key[], const value[], const ttl = 0, const type = 0, const keepttl = 0);
redis_async_set_integer(const key[], const value, const ttl = 0, const type = 0, const keepttl = 0);
redis_async_del_key(const key[]);
redis_async_hdel_field(const key[], const field[]);
redis_async_get_string(const key[], request_id = 0);
redis_async_get_integer(const key[], request_id = 0);
redis_async_hget_string(const key[], const field[], request_id = 0);
redis_async_hget_integer(const key[], const field[], request_id = 0);
redis_async_queue_size();
redis_async_queue_size_on(connection_id);
redis_async_set_queue_limit(limit);
redis_async_last_error(output[], maxlength);
redis_async_last_error_on(connection_id, output[], maxlength);
```

The legacy `redis_async_connect()` API uses default connection handle `1`.
Existing plugins can keep using the original `redis_async_*` natives unchanged.
New plugins that need independent endpoints or isolated queues should use
`redis_async_open()` and the handle-specific `*_on(connection_id, ...)` natives:

```pawn
new conn = redis_async_open("127.0.0.1", 6379, "", "secret", 200, "analytics");
redis_async_publish_on(conn, "amxx:analytics", payload);
redis_async_hset_string_on(conn, "amxx:analytics:events", event_id, payload);
```

Return value is `0` when the command is queued and `-1` when Redis is not ready
or the queue is full. Commands queued after `redis_async_connect()` wait behind
the worker-thread connection attempt; if that connection fails, async `GET` and
`HGET` calls receive an error result.

`redis_async_connect()` validates host/port before starting the worker. Invalid
parameters, duplicate async connect attempts, worker start failures, and worker
connection failures are all recorded in both `redis_async_last_error()` and
`redis_last_error()`. A `Redis_Async_OnConnect` failure callback is also queued
when possible so plugins can keep one error-handling path.

`redis_async_connect()` is safe to call from `plugin_init`. The module queues the
connect result and dispatches it only after async forwards are registered, so
plugins do not depend on `OnPluginsLoaded` ordering.

The sync Pub/Sub setup is also safe from `plugin_init`:

```pawn
redis_connect("127.0.0.1", 6379);
redis_register_subscriber("my_channel");
redis_start_subscribe();
```

Subscriber channels are preserved until `OnPluginsLoaded`; the module registers
`Redis_Subscriber_OnMessage` then starts the subscriber thread.

`redis_connect()` and all Redis natives catch Redis/client exceptions and return
`-1` on failure instead of allowing an exception to escape into HLDS. Plugins can
read the last module error with:

```pawn
new error[192];
redis_last_error(error, charsmax(error));
```

Async connect status is dispatched back on the AMXX main thread through:

```pawn
public Redis_Async_OnConnect(request_id, status, error[])
{
    if (status == 0)
    {
        server_print("Redis async connection ready");
    }
    else
    {
        server_print("Redis async connection failed: %s", error);
    }
}
```

Handle-aware connection and read callbacks are also available:

```pawn
public Redis_Async_OnConnection(connection_id, request_id, status, error[])
{
    if (status == 0) {
        server_print("Redis async handle %d is connected", connection_id);
    } else {
        server_print("Redis async handle %d failed: %s", connection_id, error);
    }
}

public Redis_Async_OnResultEx(connection_id, request_id, command[], status, key[], field[], value[])
{
    server_print("Redis async handle %d %s status=%d value=%s", connection_id, command, status, value);
}
```

Async workers reconnect automatically with bounded backoff. Commands submitted
while a handle is connecting or reconnecting are stored in that handle's bounded
queue and flushed after reconnect. Queue-full, invalid-handle, and connection
errors are available through `redis_async_last_error()` or
`redis_async_last_error_on()`.

### Async stress test

`redis_async_stress_test.sma` opens the default legacy connection plus two
independent handles. It writes isolated test data to:

```text
amxx:test:async:health
amxx:test:async:events:handle1
amxx:test:async:events:handle2
```

and publishes to:

```text
amxx:test:async:handle1
amxx:test:async:handle2
```

Configure it through these cvars before loading the plugin:

```text
redis_async_test_host
redis_async_test_port
redis_async_test_username
redis_async_test_password
redis_async_test_interval
```

Async `GET`/`HGET` results are dispatched back on the AMXX main thread through a
global forward:

```pawn
#define REQ_PLAYER_CACHE 1001

redis_async_hget_string("player:cache", authid, REQ_PLAYER_CACHE);

public Redis_Async_OnResult(request_id, command[], status, key[], field[], value[])
{
    if (request_id != REQ_PLAYER_CACHE)
    {
        return;
    }

    if (status == 0)
    {
        server_print("Redis value: %s", value);
    }
    else if (status == 1)
    {
        server_print("Redis value missing for %s/%s", key, field);
    }
    else
    {
        server_print("Redis async error: %s", value);
    }
}
```

The result status contract is:

- `0`: value found.
- `1`: Redis nil/missing value.
- `-1`: worker error, with the error message in `value`.

---
### A little test:
> I took a simple benchmark using Redis.<br/>
> In one cycle, I did a SET/GET/DEL against a hash table and processed 100,000 cases.<br/>
> The result was 14 seconds, or 0.00014 per cycle.<br/>
> Needless to say, since we ran 100,000 cycles in FOR logic, the CPU usage was 100% during that time.
```C++
server_print("[REDIS] BENCHMARK START");
new start = get_systime();
new iResult = 0;
for (new i = 0; i < 100000; i++)
{
    redis_hset_integer("REDIS_TEST_HASH", "INTEGER", i);
    iResult = redis_hget_integer("REDIS_TEST_HASH", "INTEGER");
    redis_hdel_field("REDIS_TEST_HASH", "INTEGER");
//  server_print("[REDIS] %d", iResult);
}
new bench = get_systime() - start;
server_print("[REDIS] %d", iResult);
server_print("[REDIS] BENCHMARK %d SEC", bench);
```
```
[REDIS] BENCHMARK START
[REDIS] 99999
[REDIS] BENCHMARK 14 SEC
```

---
## I need your help.
- ~~Build environment on Windows.~~
- ~~Automatic build of external libraries.~~
  -  Implemented by [MisterCalvin](https://github.com/MisterCalvin). Thanks.
- If there is a feature you would like to see, please send a request to Issue.
