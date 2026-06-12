#include <amxmodx>
#include <redis>

#define PLUGIN "Redis Async Test"
#define VERSION "0.02"
#define AUTHOR "Aoi.Kagase; samurake"
#define REQ_CONNECT 100
#define REQ_GET_TEST 101

new const HOST_IP[] = "127.0.0.1";
new const HOST_PORT = 6379;

public plugin_init()
{
    register_plugin(PLUGIN, VERSION, AUTHOR);

    redis_async_set_queue_limit(8192);
    if(redis_async_connect(HOST_IP, HOST_PORT, "", "", REQ_CONNECT) != 0)
    {
        new error[192];
        redis_async_last_error(error, charsmax(error));
        server_print("[REDIS] async connect queue failed: %s", error);
    }
}

public Redis_Async_OnConnect(request_id, status, error[])
{
    if(request_id != REQ_CONNECT)
    {
        return;
    }

    if(status != 0)
    {
        server_print("[REDIS] async connect failed: %s", error);
        return;
    }

    server_print("[REDIS] async connection ready");
    redis_async_set_string("TEST", "REDIS_123456");
    redis_async_get_string("TEST", REQ_GET_TEST);
    redis_async_del_key("TEST");
}

public Redis_Async_OnResult(request_id, command[], status, key[], field[], value[])
{
    if(request_id != REQ_GET_TEST)
    {
        return;
    }

    server_print("[REDIS] async %s key=%s field=%s status=%d value=%s", command, key, field, status, value);
}

public Redis_Subscriber_OnMessage(channel[], message[])
{
    server_print("[REDIS] subscriber channel=%s message=%s", channel, message);
}

public CompileCheckSyncSubscriberApi()
{
    redis_connect(HOST_IP, HOST_PORT);
    redis_register_subscriber("REDIS_TEST_CHANNEL");
    redis_start_subscribe();
}
