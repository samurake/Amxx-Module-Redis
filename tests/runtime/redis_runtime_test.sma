#include <amxmodx>
#include <amxmisc>
#include <redis>

#define PLUGIN "Redis Runtime Fault Gate"
#define VERSION "0.3.3"
#define AUTHOR "samurake"

#define CONNECT_REQUEST 42000
#define PROBE_REQUEST 42001
#define FLOOD_REQUEST_BASE 43000
#define FLOOD_COUNT 128
#define QUEUE_LIMIT 32
#define DEFAULT_QUEUE_LIMIT 4096

new g_host_cvar
new g_port_cvar
new g_handle
new g_accepted
new g_rejected
new g_callbacks
new g_callback_failures
new g_drops_before

public plugin_init()
{
    register_plugin(PLUGIN, VERSION, AUTHOR)
    g_host_cvar = register_cvar("redis_runtime_host", "redis", FCVAR_PROTECTED)
    g_port_cvar = register_cvar("redis_runtime_port", "6379")
    register_concmd("redis_runtime_probe", "CommandProbe", ADMIN_RCON)
    register_concmd("redis_runtime_backpressure", "CommandBackpressure", ADMIN_RCON)
    register_concmd("redis_runtime_status", "CommandStatus", ADMIN_RCON)
    register_concmd("redis_runtime_restore_defaults", "CommandRestoreDefaults", ADMIN_RCON)

    OpenConnection()
}

public plugin_end()
{
    if(g_handle > 0)
    {
        redis_async_close(g_handle)
        g_handle = 0
    }
}

stock OpenConnection()
{
    new host[64]
    get_pcvar_string(g_host_cvar, host, charsmax(host))
    g_handle = redis_async_open(
        host,
        get_pcvar_num(g_port_cvar),
        "",
        "",
        CONNECT_REQUEST,
        "runtime-fault-gate"
    )
    if(g_handle <= 0)
    {
        server_print("[Redis Runtime][FAIL] could not open async handle")
    }
}

public CommandProbe(id, level, cid)
{
    if(id && !cmd_access(id, level, cid, 1))
    {
        return PLUGIN_HANDLED
    }

    new result = redis_async_xadd_on(
        g_handle,
        "amxx:test:runtime",
        "runtime:probe",
        "{^"kind^":^"probe^"}",
        PROBE_REQUEST
    )
    server_print(
        "[Redis Runtime][PROBE_QUEUED] result=%s return=%d",
        result == 0 ? "PASS" : "FAIL",
        result
    )
    return PLUGIN_HANDLED
}

public CommandBackpressure(id, level, cid)
{
    if(id && !cmd_access(id, level, cid, 1))
    {
        return PLUGIN_HANDLED
    }

    g_accepted = 0
    g_rejected = 0
    g_callbacks = 0
    g_callback_failures = 0
    g_drops_before = redis_async_dropped_results()

    new limit_result = redis_async_set_queue_limit(QUEUE_LIMIT)

    new event_id[48]
    new payload[96]
    for(new index = 0; index < FLOOD_COUNT; index++)
    {
        formatex(event_id, charsmax(event_id), "runtime:outage:%d", index)
        formatex(payload, charsmax(payload), "{^"kind^":^"outage^",^"index^":%d}", index)
        new result = redis_async_xadd_on(
            g_handle,
            "amxx:test:runtime",
            event_id,
            payload,
            FLOOD_REQUEST_BASE + index
        )
        if(result == 0)
        {
            g_accepted++
        }
        else
        {
            g_rejected++
        }
    }

    new queue_size = redis_async_queue_size_on(g_handle)
    new queue_bytes = redis_async_queue_bytes_on(g_handle)
    new bool:passed = limit_result == 0
        && g_rejected > 0
        && g_accepted <= QUEUE_LIMIT
        && queue_size <= QUEUE_LIMIT
        && queue_bytes <= REDIS_ASYNC_DEFAULT_QUEUE_BYTE_LIMIT

    server_print(
        "[Redis Runtime][BACKPRESSURE] result=%s accepted=%d rejected=%d queue=%d bytes=%d",
        passed ? "PASS" : "FAIL",
        g_accepted,
        g_rejected,
        queue_size,
        queue_bytes
    )
    return PLUGIN_HANDLED
}

public CommandRestoreDefaults(id, level, cid)
{
    if(id && !cmd_access(id, level, cid, 1))
    {
        return PLUGIN_HANDLED
    }

    new result = redis_async_set_queue_limit(DEFAULT_QUEUE_LIMIT)
    server_print(
        "[Redis Runtime][DEFAULTS] result=%s queue_limit=%d",
        result == 0 ? "PASS" : "FAIL",
        DEFAULT_QUEUE_LIMIT
    )
    return PLUGIN_HANDLED
}

public CommandStatus(id, level, cid)
{
    if(id && !cmd_access(id, level, cid, 1))
    {
        return PLUGIN_HANDLED
    }

    new queue_size = redis_async_queue_size_on(g_handle)
    new queue_bytes = redis_async_queue_bytes_on(g_handle)
    new drops = redis_async_dropped_results() - g_drops_before
    new bool:passed = g_accepted > 0
        && g_callbacks == g_accepted
        && g_callback_failures == 0
        && queue_size == 0
        && queue_bytes == 0
        && drops == 0
        && redis_async_status(g_handle) == REDIS_ASYNC_STATUS_CONNECTED

    server_print(
        "[Redis Runtime][RECOVERY] result=%s status=%d accepted=%d callbacks=%d callback_failures=%d queue=%d bytes=%d drops=%d",
        passed ? "PASS" : "WAIT",
        redis_async_status(g_handle),
        g_accepted,
        g_callbacks,
        g_callback_failures,
        queue_size,
        queue_bytes,
        drops
    )
    return PLUGIN_HANDLED
}

public Redis_Async_OnConnection(connection_id, request_id, status, error[])
{
    if(connection_id != g_handle)
    {
        return
    }

    server_print(
        "[Redis Runtime][CONNECTION] request=%d status=%d error=%s",
        request_id,
        status,
        error[0] ? error : "none"
    )
}

public Redis_Async_OnResultEx(
    connection_id,
    request_id,
    command[],
    status,
    key[],
    field[],
    value[]
)
{
    if(connection_id != g_handle || !equal(command, "xadd"))
    {
        return
    }

    if(request_id == PROBE_REQUEST)
    {
        server_print(
            "[Redis Runtime][PROBE_CALLBACK] result=%s status=%d stream_id=%s",
            status == 0 && value[0] ? "PASS" : "FAIL",
            status,
            value
        )
        return
    }

    if(request_id >= FLOOD_REQUEST_BASE
        && request_id < FLOOD_REQUEST_BASE + FLOOD_COUNT)
    {
        g_callbacks++
        if(status != 0 || !value[0] || !equal(key, "amxx:test:runtime"))
        {
            g_callback_failures++
        }
    }
}
