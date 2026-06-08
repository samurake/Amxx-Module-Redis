#include "module.h"
int ForwardRedisOnMessage = -1;
int ForwardRedisAsyncOnResult = -1;
int HasRedisOnMessage = -1;

void OnAmxxAttach()
{
    MF_AddNatives(g_natives);
}

void OnPluginsLoaded()
{
	isSubscriberRunning = false;
	ForwardRedisOnMessage = MF_RegisterForward("Redis_Subscriber_OnMessage", ET_STOP, FP_STRING, FP_STRING, FP_DONE);
	ForwardRedisAsyncOnResult = MF_RegisterForward("Redis_Async_OnResult", ET_IGNORE, FP_CELL, FP_STRING, FP_CELL, FP_STRING, FP_STRING, FP_STRING, FP_DONE);
	HasRedisOnMessage = UTIL_CheckForPublic("Redis_Subscriber_OnMessage");

	if (g_redis)
	{
		redis_register_subscriber_forward(HasRedisOnMessage);
		redis_start_subscribe(HasRedisOnMessage);
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
	if (!g_redis)
	{
		return;
	}

	g_redis->bgsave();

	delete g_redis;
	g_redis = nullptr;
}

