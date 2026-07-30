#include <amxmodx>
#include <redis>

#define PLUGIN "Redis Async Stress Test"
#define VERSION "0.1"
#define AUTHOR "samurake"

#define REQ_DEFAULT_CONNECT 7000
#define REQ_DEFAULT_DUPLICATE 7001
#define REQ_HANDLE1_CONNECT 7101
#define REQ_HANDLE2_CONNECT 7102
#define REQ_HANDLE1_GET 7201
#define REQ_HANDLE2_GET 7202
#define REQ_HANDLE1_HGET 7301
#define REQ_HANDLE2_HGET 7302
#define REQ_HANDLE1_XADD 7401
#define REQ_HANDLE2_XADD 7402

new redis_async_test_host
new redis_async_test_port
new redis_async_test_username
new redis_async_test_password
new redis_async_test_interval

new g_handle1
new g_handle2
new g_default_connected
new g_handle1_connected
new g_handle2_connected
new g_tick

public plugin_init()
{
    register_plugin(PLUGIN, VERSION, AUTHOR)

    redis_async_test_host = register_cvar("redis_async_test_host", "127.0.0.1", FCVAR_PROTECTED)
    redis_async_test_port = register_cvar("redis_async_test_port", "6379", FCVAR_PROTECTED)
    redis_async_test_username = register_cvar("redis_async_test_username", "", FCVAR_PROTECTED)
    redis_async_test_password = register_cvar("redis_async_test_password", "", FCVAR_PROTECTED)
    redis_async_test_interval = register_cvar("redis_async_test_interval", "10.0")

    redis_async_set_queue_limit(8192)
}

public plugin_cfg()
{
    new host[64], username[64], password[128]
    get_pcvar_string(redis_async_test_host, host, charsmax(host))
    get_pcvar_string(redis_async_test_username, username, charsmax(username))
    get_pcvar_string(redis_async_test_password, password, charsmax(password))

    new port = get_pcvar_num(redis_async_test_port)
    server_print("[Redis Async Stress] opening default and handle connections host=%s port=%d", host, port)

    if(redis_async_connect(host, port, username, password, REQ_DEFAULT_CONNECT) != 0)
    {
        PrintLastError(0, "legacy default connect")
    }

    if(redis_async_connect(host, port, username, password, REQ_DEFAULT_DUPLICATE) != 0)
    {
        PrintLastError(0, "legacy duplicate connect")
    }

    g_handle1 = redis_async_open(host, port, username, password, REQ_HANDLE1_CONNECT, "stress-handle1")
    if(g_handle1 <= 0)
    {
        PrintLastError(0, "handle1 open")
    }

    g_handle2 = redis_async_open(host, port, username, password, REQ_HANDLE2_CONNECT, "stress-handle2")
    if(g_handle2 <= 0)
    {
        PrintLastError(0, "handle2 open")
    }

    set_task(FloatMax(get_pcvar_float(redis_async_test_interval), 2.0), "StressTick", _, _, _, "b")
}

public plugin_end()
{
    if(g_handle1 > 0)
    {
        redis_async_close(g_handle1)
    }

    if(g_handle2 > 0)
    {
        redis_async_close(g_handle2)
    }
}

public Redis_Async_OnConnect(request_id, status, error[])
{
    if(request_id == REQ_DEFAULT_CONNECT || request_id == REQ_DEFAULT_DUPLICATE)
    {
        if(status == 0)
        {
            g_default_connected = 1
            server_print("[Redis Async Stress][PASS] legacy connect callback request=%d status=%d", request_id, status)
        }
        else
        {
            server_print("[Redis Async Stress][FAIL] legacy connect callback request=%d status=%d error=%s", request_id, status, error)
        }
    }
}

public Redis_Async_OnConnection(connection_id, request_id, status, error[])
{
    if(request_id == REQ_HANDLE1_CONNECT)
    {
        g_handle1_connected = status == 0 ? 1 : 0
        server_print("[Redis Async Stress][%s] handle1 connection_id=%d status=%d error=%s", status == 0 ? "PASS" : "FAIL", connection_id, status, error)
    }
    else if(request_id == REQ_HANDLE2_CONNECT)
    {
        g_handle2_connected = status == 0 ? 1 : 0
        server_print("[Redis Async Stress][%s] handle2 connection_id=%d status=%d error=%s", status == 0 ? "PASS" : "FAIL", connection_id, status, error)
    }
    else if(request_id == REQ_DEFAULT_CONNECT || request_id == REQ_DEFAULT_DUPLICATE)
    {
        server_print("[Redis Async Stress] default handle connection_id=%d request=%d status=%d error=%s", connection_id, request_id, status, error)
    }
}

public Redis_Async_OnResult(request_id, command[], status, key[], field[], value[])
{
    if(request_id == REQ_DEFAULT_CONNECT)
    {
        server_print("[Redis Async Stress] legacy result command=%s status=%d key=%s field=%s value=%s", command, status, key, field, value)
    }
}

public Redis_Async_OnResultEx(connection_id, request_id, command[], status, key[], field[], value[])
{
    if(request_id == REQ_HANDLE1_GET || request_id == REQ_HANDLE2_GET
        || request_id == REQ_HANDLE1_HGET || request_id == REQ_HANDLE2_HGET
        || request_id == REQ_HANDLE1_XADD || request_id == REQ_HANDLE2_XADD)
    {
        server_print("[Redis Async Stress][%s] result connection_id=%d request=%d command=%s status=%d key=%s field=%s value=%s",
            status == 0 ? "PASS" : "FAIL",
            connection_id,
            request_id,
            command,
            status,
            key,
            field,
            value)
    }
}

public StressTick()
{
    g_tick++

    new timestamp = get_systime()
    new payload[256]
    formatex(payload, charsmax(payload), "{^"tick^":%d,^"timestamp^":%d,^"default_connected^":%d,^"handle1^":%d,^"handle2^":%d}",
        g_tick,
        timestamp,
        g_default_connected,
        g_handle1_connected,
        g_handle2_connected)

    if(g_default_connected)
    {
        redis_async_set_string("amxx:test:async:health", payload)
    }

    if(g_handle1 > 0)
    {
        new eventId[64]
        formatex(eventId, charsmax(eventId), "handle1:%d", g_tick)
        redis_async_xadd_on(g_handle1, "amxx:test:async:stream:handle1", eventId, payload, REQ_HANDLE1_XADD)
        redis_async_publish_on(g_handle1, "amxx:test:async:handle1", payload)
        redis_async_hset_string_on(g_handle1, "amxx:test:async:events:handle1", "last", payload)
        redis_async_hset_integer_on(g_handle1, "amxx:test:async:events:handle1", "last_timestamp", timestamp)
        redis_async_get_string_on(g_handle1, "amxx:test:async:health", REQ_HANDLE1_GET)
        redis_async_hget_string_on(g_handle1, "amxx:test:async:events:handle1", "last", REQ_HANDLE1_HGET)
    }

    if(g_handle2 > 0)
    {
        new eventId[64]
        formatex(eventId, charsmax(eventId), "handle2:%d", g_tick)
        redis_async_xadd_on(g_handle2, "amxx:test:async:stream:handle2", eventId, payload, REQ_HANDLE2_XADD)
        redis_async_publish_on(g_handle2, "amxx:test:async:handle2", payload)
        redis_async_hset_string_on(g_handle2, "amxx:test:async:events:handle2", "last", payload)
        redis_async_hset_integer_on(g_handle2, "amxx:test:async:events:handle2", "last_timestamp", timestamp)
        redis_async_get_string_on(g_handle2, "amxx:test:async:health", REQ_HANDLE2_GET)
        redis_async_hget_string_on(g_handle2, "amxx:test:async:events:handle2", "last", REQ_HANDLE2_HGET)
    }

    server_print("[Redis Async Stress] tick=%d default=%d handle1=%d handle2=%d q1=%d q2=%d",
        g_tick,
        g_default_connected,
        redis_async_status(g_handle1),
        redis_async_status(g_handle2),
        redis_async_queue_size_on(g_handle1),
        redis_async_queue_size_on(g_handle2))
}

stock PrintLastError(connection_id, const label[])
{
    new error[192]
    if(connection_id > 0)
    {
        redis_async_last_error_on(connection_id, error, charsmax(error))
    }
    else
    {
        redis_async_last_error(error, charsmax(error))
    }

    server_print("[Redis Async Stress][FAIL] %s: %s", label, error)
}

stock Float:FloatMax(Float:left, Float:right)
{
    return left > right ? left : right
}
