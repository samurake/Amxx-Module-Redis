#include "module.h"
#include "async_forward_lifecycle.h"
int ForwardRedisOnMessage = -1;
int ForwardRedisAsyncOnResult = -1;
int ForwardRedisAsyncOnConnect = -1;
int ForwardRedisAsyncOnResultEx = -1;
int ForwardRedisAsyncOnConnection = -1;
int HasRedisOnMessage = -1;
redis_async_lifecycle::ForwardRegistry RedisForwardRegistry;

void redis_register_async_forwards()
{
	if (!RedisForwardRegistry.should_register_async())
	{
		return;
	}

	ForwardRedisAsyncOnResult = MF_RegisterForward("Redis_Async_OnResult", ET_IGNORE, FP_CELL, FP_STRING, FP_CELL, FP_STRING, FP_STRING, FP_STRING, FP_DONE);
	ForwardRedisAsyncOnConnect = MF_RegisterForward("Redis_Async_OnConnect", ET_IGNORE, FP_CELL, FP_CELL, FP_STRING, FP_DONE);
	ForwardRedisAsyncOnResultEx = MF_RegisterForward("Redis_Async_OnResultEx", ET_IGNORE, FP_CELL, FP_CELL, FP_STRING, FP_CELL, FP_STRING, FP_STRING, FP_STRING, FP_DONE);
	ForwardRedisAsyncOnConnection = MF_RegisterForward("Redis_Async_OnConnection", ET_IGNORE, FP_CELL, FP_CELL, FP_CELL, FP_STRING, FP_DONE);
	RedisForwardRegistry.mark_async_registered();
}

void OnAmxxAttach()
{
    MF_AddNatives(g_natives);
}

void OnPluginsLoaded()
{
	RedisForwardRegistry.on_plugins_loaded();
	isSubscriberRunning = false;
	if (RedisForwardRegistry.should_register_subscriber())
	{
		ForwardRedisOnMessage = MF_RegisterForward("Redis_Subscriber_OnMessage", ET_STOP, FP_STRING, FP_STRING, FP_DONE);
		RedisForwardRegistry.mark_subscriber_registered();
	}
	redis_register_async_forwards();
	HasRedisOnMessage = UTIL_CheckForPublic("Redis_Subscriber_OnMessage");

	if (g_redis)
	{
		redis_register_subscriber_forward(HasRedisOnMessage);
		redis_start_subscribe_now(HasRedisOnMessage);
	}
}

void stop_subscribe()
{
	isSubscriberRunning = false;

	if (th_subscriber && th_subscriber->joinable()) {
		th_subscriber->join();
	}

	if (sub) {
		sub->unsubscribe();
		delete sub;
		sub = nullptr;
	}

	if (g_subscriber_redis) {
		delete g_subscriber_redis;
		g_subscriber_redis = nullptr;
	}
}

void OnPluginsUnloading()
{
	// AMXX recreates plugin instances and their global-forward bindings on a
	// map/plugin reload. Stop producing results and require fresh forward IDs
	// for the next plugin generation.
	RedisForwardRegistry.on_plugins_unloading();
	redis_stop_async_worker();
}

void StartFrame()
{
	redis_dispatch_async_results();
}

void OnPluginsUnloaded()
{
	stop_subscribe();
	redis_stop_async_worker();

	channels.clear();
	ForwardRedisOnMessage = -1;
	ForwardRedisAsyncOnResult = -1;
	ForwardRedisAsyncOnConnect = -1;
	ForwardRedisAsyncOnResultEx = -1;
	ForwardRedisAsyncOnConnection = -1;
	HasRedisOnMessage = -1;

	if (!g_redis)
	{
		return;
	}

	delete g_redis;
	g_redis = nullptr;
}

