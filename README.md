# AMX MOD X - Redis Client Module
### Description:
> This is a module for amxmodx that allows operations from HLDS to the Redis data store.

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
AMXX/game thread. Use the `redis_async_*` natives for runtime paths that must not
wait on network I/O, such as analytics publishing or write-behind snapshots.

The async API starts a worker thread after `redis_connect()` succeeds. Async
write natives only copy Pawn parameters into a bounded queue and return:

```pawn
redis_connect("127.0.0.1", 6379, "", "secret");
redis_async_set_queue_limit(8192);

redis_async_publish("amxx:server_analytics", payload);
redis_async_hset_string("amxx:server_analytics:events:2026-06-08", event_id, payload);
```

Available async write/read natives:

```pawn
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
redis_async_set_queue_limit(limit);
redis_async_last_error(output[], maxlength);
```

Return value is `0` when the command is queued and `-1` when Redis is not ready
or the queue is full. The worker owns a separate Redis connection, so it does not
share redis-plus-plus connection state with the game thread.

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
