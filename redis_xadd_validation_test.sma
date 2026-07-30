#include <amxmodx>
#include <amxmisc>
#include <redis>

#define PLUGIN "Redis XADD Validation"
#define VERSION "0.3.1"
#define AUTHOR "samurake"

#define CONNECT_BOUNDARY_REQUEST 9001
#define CONNECT_RATE_REQUEST 9002

#define REQUEST_DUPLICATE 9101
#define REQUEST_MAX_STREAM 9102
#define REQUEST_MAX_EVENT 9103
#define REQUEST_MAX_PAYLOAD 9104

#define RATE_REQUEST_BASE 10000
#define RATE_ACCEPTED_COUNT 256

#define VALIDATION_STREAM "amxx:test:xadd:validation"
#define RATE_STREAM "amxx:test:xadd:rate"

new g_host_cvar
new g_port_cvar
new g_username_cvar
new g_password_cvar

new g_boundary_handle
new g_rate_handle
new bool:g_boundary_connected
new bool:g_rate_connected
new bool:g_validation_running
new bool:g_tests_started

new g_assertions
new g_failures
new g_boundary_callbacks
new g_rate_callbacks
new g_dropped_results_before

new g_max_stream[REDIS_ASYNC_XADD_MAX_STREAM_BYTES + 1]
new g_too_large_stream[REDIS_ASYNC_XADD_MAX_STREAM_BYTES + 2]
new g_max_event[REDIS_ASYNC_XADD_MAX_EVENT_ID_BYTES + 1]
new g_too_large_event[REDIS_ASYNC_XADD_MAX_EVENT_ID_BYTES + 2]
new g_max_payload[REDIS_ASYNC_XADD_MAX_PAYLOAD_BYTES + 1]
new g_too_large_payload[REDIS_ASYNC_XADD_MAX_PAYLOAD_BYTES + 2]

public plugin_init()
{
    register_plugin(PLUGIN, VERSION, AUTHOR)

    g_host_cvar = register_cvar(
        "redis_xadd_validation_host",
        "127.0.0.1",
        FCVAR_PROTECTED
    )
    g_port_cvar = register_cvar("redis_xadd_validation_port", "6379")
    g_username_cvar = register_cvar(
        "redis_xadd_validation_username",
        "",
        FCVAR_PROTECTED
    )
    g_password_cvar = register_cvar(
        "redis_xadd_validation_password",
        "",
        FCVAR_PROTECTED
    )

    register_concmd(
        "redis_xadd_validate",
        "CommandRunValidation",
        ADMIN_RCON,
        "- run the bounded async XADD validation suite"
    )

    FillBuffer(g_max_stream, REDIS_ASYNC_XADD_MAX_STREAM_BYTES, 's')
    FillBuffer(
        g_too_large_stream,
        REDIS_ASYNC_XADD_MAX_STREAM_BYTES + 1,
        's'
    )
    FillBuffer(g_max_event, REDIS_ASYNC_XADD_MAX_EVENT_ID_BYTES, 'e')
    FillBuffer(
        g_too_large_event,
        REDIS_ASYNC_XADD_MAX_EVENT_ID_BYTES + 1,
        'e'
    )
    FillBuffer(g_max_payload, REDIS_ASYNC_XADD_MAX_PAYLOAD_BYTES, 'p')
    FillBuffer(
        g_too_large_payload,
        REDIS_ASYNC_XADD_MAX_PAYLOAD_BYTES + 1,
        'p'
    )
}

public plugin_end()
{
    CloseValidationHandles()
}

public CommandRunValidation(id, level, cid)
{
    if(id && !cmd_access(id, level, cid, 1))
    {
        return PLUGIN_HANDLED
    }

    if(g_validation_running)
    {
        console_print(id, "[Redis XADD Validation] a validation run is active")
        return PLUGIN_HANDLED
    }

    StartValidation(id)
    return PLUGIN_HANDLED
}

stock StartValidation(console_id)
{
    CloseValidationHandles()
    remove_task(92001)

    g_boundary_connected = false
    g_rate_connected = false
    g_validation_running = true
    g_tests_started = false
    g_assertions = 0
    g_failures = 0
    g_boundary_callbacks = 0
    g_rate_callbacks = 0
    g_dropped_results_before = redis_async_dropped_results()

    new host[64], username[64], password[128]
    get_pcvar_string(g_host_cvar, host, charsmax(host))
    get_pcvar_string(g_username_cvar, username, charsmax(username))
    get_pcvar_string(g_password_cvar, password, charsmax(password))
    new port = get_pcvar_num(g_port_cvar)

    g_boundary_handle = redis_async_open(
        host,
        port,
        username,
        password,
        CONNECT_BOUNDARY_REQUEST,
        "xadd-validation-boundary"
    )
    if(g_boundary_handle <= 0)
    {
        ValidationFailure("could not open boundary connection", 0)
    }

    g_rate_handle = redis_async_open(
        host,
        port,
        username,
        password,
        CONNECT_RATE_REQUEST,
        "xadd-validation-rate"
    )
    if(g_rate_handle <= 0)
    {
        ValidationFailure("could not open rate connection", 0)
    }

    if(g_boundary_handle <= 0 || g_rate_handle <= 0)
    {
        PrintLastError(console_id, 0, "connection setup")
        FinishValidation()
        return
    }

    server_print(
        "[Redis XADD Validation] started boundary_handle=%d rate_handle=%d",
        g_boundary_handle,
        g_rate_handle
    )
    set_task(30.0, "ValidationTimeout", 92001)
}

public Redis_Async_OnConnection(
    connection_id,
    request_id,
    status,
    error[]
)
{
    if(!g_validation_running)
    {
        return
    }

    if(request_id == CONNECT_BOUNDARY_REQUEST)
    {
        AssertTrue(
            "boundary connection callback handle",
            connection_id == g_boundary_handle
        )
        AssertTrue("boundary connection status", status == 0)
        if(status != 0)
        {
            server_print(
                "[Redis XADD Validation][FAIL] boundary connection: %s",
                error
            )
            FinishValidation()
            return
        }
        g_boundary_connected = true
    }
    else if(request_id == CONNECT_RATE_REQUEST)
    {
        AssertTrue(
            "rate connection callback handle",
            connection_id == g_rate_handle
        )
        AssertTrue("rate connection status", status == 0)
        if(status != 0)
        {
            server_print(
                "[Redis XADD Validation][FAIL] rate connection: %s",
                error
            )
            FinishValidation()
            return
        }
        g_rate_connected = true
    }
    else
    {
        return
    }

    if(g_boundary_connected && g_rate_connected && !g_tests_started)
    {
        g_tests_started = true
        set_task(0.2, "RunBoundaryTests")
        set_task(1.2, "RunRateTests")
    }
}

public RunBoundaryTests()
{
    if(!g_validation_running || !g_boundary_connected)
    {
        return
    }

    AssertRejected(
        "empty stream",
        redis_async_xadd_on(
            g_boundary_handle,
            "",
            "validation:empty-stream",
            "{}",
            9201
        ),
        "stream is empty"
    )
    AssertRejected(
        "oversized stream",
        redis_async_xadd_on(
            g_boundary_handle,
            g_too_large_stream,
            "validation:large-stream",
            "{}",
            9202
        ),
        "stream exceeds"
    )
    AssertRejected(
        "empty event id",
        redis_async_xadd_on(
            g_boundary_handle,
            VALIDATION_STREAM,
            "",
            "{}",
            9203
        ),
        "event_id is empty"
    )
    AssertRejected(
        "oversized event id",
        redis_async_xadd_on(
            g_boundary_handle,
            VALIDATION_STREAM,
            g_too_large_event,
            "{}",
            9204
        ),
        "event_id exceeds"
    )
    AssertRejected(
        "empty payload",
        redis_async_xadd_on(
            g_boundary_handle,
            VALIDATION_STREAM,
            "validation:empty-payload",
            "",
            9205
        ),
        "payload is empty"
    )
    AssertRejected(
        "oversized payload",
        redis_async_xadd_on(
            g_boundary_handle,
            VALIDATION_STREAM,
            "validation:large-payload",
            g_too_large_payload,
            9206
        ),
        "payload exceeds"
    )
    AssertRejected(
        "negative request id",
        redis_async_xadd_on(
            g_boundary_handle,
            VALIDATION_STREAM,
            "validation:negative-request",
            "{}",
            -1
        ),
        "zero or positive"
    )

    new result = redis_async_xadd_on(
        g_boundary_handle,
        VALIDATION_STREAM,
        "validation:duplicate",
        "{}",
        REQUEST_DUPLICATE
    )
    AssertTrue("first duplicate-id submission accepted", result == 0)
    AssertRejected(
        "duplicate pending request id",
        redis_async_xadd_on(
            g_boundary_handle,
            VALIDATION_STREAM,
            "validation:duplicate",
            "{}",
            REQUEST_DUPLICATE
        ),
        "already pending"
    )

    result = redis_async_xadd_on(
        g_boundary_handle,
        g_max_stream,
        "validation:max-stream",
        "{}",
        REQUEST_MAX_STREAM
    )
    AssertTrue("191-byte stream accepted", result == 0)

    result = redis_async_xadd_on(
        g_boundary_handle,
        VALIDATION_STREAM,
        g_max_event,
        "{}",
        REQUEST_MAX_EVENT
    )
    AssertTrue("191-byte event id accepted", result == 0)

    result = redis_async_xadd_on(
        g_boundary_handle,
        VALIDATION_STREAM,
        "validation:max-payload",
        g_max_payload,
        REQUEST_MAX_PAYLOAD
    )
    AssertTrue("8191-byte payload accepted", result == 0)
}

public RunRateTests()
{
    if(!g_validation_running || !g_rate_connected)
    {
        return
    }

    new accepted
    new rejected
    new event_id[48]
    new payload[64]

    for(new index = 0; index <= RATE_ACCEPTED_COUNT; index++)
    {
        formatex(event_id, charsmax(event_id), "validation:rate:%d", index)
        formatex(payload, charsmax(payload), "{^"index^":%d}", index)

        new result = redis_async_xadd_on(
            g_rate_handle,
            RATE_STREAM,
            event_id,
            payload,
            RATE_REQUEST_BASE + index
        )
        if(result == 0)
        {
            accepted++
        }
        else
        {
            rejected++
        }
    }

    AssertTrue(
        "rate limiter accepts first 256 submissions",
        accepted == RATE_ACCEPTED_COUNT
    )
    AssertTrue("rate limiter rejects submission 257", rejected == 1)

    new error[192]
    redis_async_last_error_on(g_rate_handle, error, charsmax(error))
    AssertTrue(
        "rate limiter reports explicit error",
        containi(error, "rate limit") >= 0
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
    if(!g_validation_running)
    {
        return
    }

    if(request_id >= RATE_REQUEST_BASE
        && request_id < RATE_REQUEST_BASE + RATE_ACCEPTED_COUNT)
    {
        new expected_event[48]
        formatex(
            expected_event,
            charsmax(expected_event),
            "validation:rate:%d",
            request_id - RATE_REQUEST_BASE
        )

        new valid = connection_id == g_rate_handle
            && equal(command, "xadd")
            && status == 0
            && equal(key, RATE_STREAM)
            && equal(field, expected_event)
            && IsRedisStreamId(value)
        AssertTrue("rate callback correlation", valid)
        g_rate_callbacks++
        MaybeFinishValidation()
        return
    }

    new expected_key[REDIS_ASYNC_XADD_MAX_STREAM_BYTES + 1]
    new expected_field[REDIS_ASYNC_XADD_MAX_EVENT_ID_BYTES + 1]
    if(request_id == REQUEST_DUPLICATE)
    {
        copy(expected_key, charsmax(expected_key), VALIDATION_STREAM)
        copy(expected_field, charsmax(expected_field), "validation:duplicate")
    }
    else if(request_id == REQUEST_MAX_STREAM)
    {
        copy(expected_key, charsmax(expected_key), g_max_stream)
        copy(expected_field, charsmax(expected_field), "validation:max-stream")
    }
    else if(request_id == REQUEST_MAX_EVENT)
    {
        copy(expected_key, charsmax(expected_key), VALIDATION_STREAM)
        copy(expected_field, charsmax(expected_field), g_max_event)
    }
    else if(request_id == REQUEST_MAX_PAYLOAD)
    {
        copy(expected_key, charsmax(expected_key), VALIDATION_STREAM)
        copy(expected_field, charsmax(expected_field), "validation:max-payload")
    }
    else
    {
        return
    }

    new valid = connection_id == g_boundary_handle
        && equal(command, "xadd")
        && status == 0
        && equal(key, expected_key)
        && equal(field, expected_field)
        && IsRedisStreamId(value)
    AssertTrue("boundary callback correlation", valid)
    g_boundary_callbacks++
    MaybeFinishValidation()
}

stock MaybeFinishValidation()
{
    if(g_boundary_callbacks == 4
        && g_rate_callbacks == RATE_ACCEPTED_COUNT)
    {
        AssertTrue(
            "boundary pending command count returns to zero",
            redis_async_queue_size_on(g_boundary_handle) == 0
        )
        AssertTrue(
            "boundary queued bytes return to zero",
            redis_async_queue_bytes_on(g_boundary_handle) == 0
        )
        AssertTrue(
            "rate pending command count returns to zero",
            redis_async_queue_size_on(g_rate_handle) == 0
        )
        AssertTrue(
            "rate queued bytes return to zero",
            redis_async_queue_bytes_on(g_rate_handle) == 0
        )
        AssertTrue(
            "module result queues did not drop a callback",
            redis_async_dropped_results() == g_dropped_results_before
        )
        FinishValidation()
    }
}

public ValidationTimeout()
{
    if(!g_validation_running)
    {
        return
    }

    ValidationFailure("validation timed out waiting for callbacks", 0)
    server_print(
        "[Redis XADD Validation] callbacks boundary=%d/4 rate=%d/%d",
        g_boundary_callbacks,
        g_rate_callbacks,
        RATE_ACCEPTED_COUNT
    )
    FinishValidation()
}

stock FinishValidation()
{
    if(!g_validation_running)
    {
        return
    }

    remove_task(92001)
    server_print(
        "[Redis XADD Validation][SUMMARY] assertions=%d failures=%d result=%s",
        g_assertions,
        g_failures,
        g_failures == 0 ? "PASS" : "FAIL"
    )

    g_validation_running = false
    CloseValidationHandles()
}

stock CloseValidationHandles()
{
    if(g_boundary_handle > 0)
    {
        redis_async_close(g_boundary_handle)
        g_boundary_handle = 0
    }
    if(g_rate_handle > 0)
    {
        redis_async_close(g_rate_handle)
        g_rate_handle = 0
    }
}

stock AssertRejected(
    const label[],
    result,
    const expected_error[]
)
{
    new error[192]
    if(g_boundary_handle > 0)
    {
        redis_async_last_error_on(
            g_boundary_handle,
            error,
            charsmax(error)
        )
    }
    else
    {
        redis_async_last_error(error, charsmax(error))
    }

    AssertTrue(label, result == -1 && containi(error, expected_error) >= 0)
    if(result != -1 || containi(error, expected_error) < 0)
    {
        server_print(
            "[Redis XADD Validation] %s result=%d error=%s",
            label,
            result,
            error
        )
    }
}

stock AssertTrue(const label[], condition)
{
    g_assertions++
    if(condition)
    {
        server_print("[Redis XADD Validation][PASS] %s", label)
        return
    }

    ValidationFailure(label, 1)
}

stock ValidationFailure(const label[], counted_assertion)
{
    if(!counted_assertion)
    {
        g_assertions++
    }
    g_failures++
    server_print("[Redis XADD Validation][FAIL] %s", label)
}

stock PrintLastError(console_id, connection_id, const label[])
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
    console_print(
        console_id,
        "[Redis XADD Validation][FAIL] %s: %s",
        label,
        error
    )
}

stock FillBuffer(buffer[], length, value)
{
    for(new index = 0; index < length; index++)
    {
        buffer[index] = value
    }
    buffer[length] = EOS
}

stock IsRedisStreamId(const value[])
{
    new separator = contain(value, "-")
    if(separator < 1 || !value[separator + 1])
    {
        return 0
    }

    for(new index = 0; value[index] != EOS; index++)
    {
        if(index == separator)
        {
            continue
        }
        if(value[index] < '0' || value[index] > '9')
        {
            return 0
        }
    }
    return 1
}
