#include "module.h"

using namespace sw::redis;

std::vector<std::string> channels;
std::thread *th_subscriber = NULL;
bool isSubscriberRunning = false;
ConnectionOptions g_subscriber_options;

cell redis_register_subscriber_forward(bool hasOnMessage)
{
	if (hasOnMessage)
	{
		try
		{
			channels.clear();
			g_subscriber_options = g_connection_options;
			g_subscriber_options.socket_timeout = std::chrono::milliseconds(300);
			g_subscriber_redis = new Redis(g_subscriber_options);
			g_subscriber_redis->ping();
			sub = new Subscriber(g_subscriber_redis->subscriber());
		}
		catch (const Error& e)
		{
			redis_set_last_error(e.what());
			MF_Log("[Redis] subscriber connection failed: %s", e.what());
			return -1;
		}
		catch (const std::exception& e)
		{
			redis_set_last_error(e.what());
			MF_Log("[Redis] subscriber connection failed: %s", e.what());
			return -1;
		}
		catch (...)
		{
			redis_set_last_error("unknown Redis subscriber connection error");
			MF_Log("[Redis] subscriber connection failed: unknown Redis subscriber connection error");
			return -1;
		}

		// Set callback functions.
		sub->on_message([](std::string channel, std::string msg) {
#if DEBUG_LOGGING
			MF_Log("[REDIS:DEBUG] ON_MESSAGE: channel='%s', message='%s'", channel.c_str(), msg.c_str());
#endif
			// Process message of MESSAGE type.
			MF_ExecuteForward(ForwardRedisOnMessage, channel.c_str(), msg.c_str());
			});
	}
	return 0;
}

// native redis_subscribe(const channel[]);
cell redis_register_subscriber(AMX *amx, cell *params)
{
	//if (!HasRedisOnMessage)
	//	return -1;

	int len = 0;
	std::string channel = MF_GetAmxString(amx, params[1], 0, &len);

	if (g_redis != NULL)
	{
		channels.push_back(channel);
	}
	else
		return -1;

	return 0;
}

void consumeThread()
{
	isSubscriberRunning = true;

	while (isSubscriberRunning)
	{
		try
		{
			sub->consume();
		}
		catch (const TimeoutError& e)
		{
			// Do nothing, as we expect a timeout, as we set socket_timeout.
			LOG_CONSOLE(PLID, "[DEBUG] SUBSCRIBE TIMEOUT: %s", e.what());
			continue;
		}
		catch (const Error& err)
		{
			LOG_CONSOLE(PLID, "[DEBUG] SUBSCRIBE ERROR: %s", err.what());
			return;
		}
	}
}

// native redis_start_subscribe();
cell redis_start_subscribe(bool hasOnMessage)
{
	if (!hasOnMessage)
	{
		MF_Log("[WARN] NOT EXISTS FORWARD. EXIT.");
		return -1;
	}

	if (channels.size() > 0)
	{
		if (sub == nullptr)
		{
			MF_Log("[WARN] REDIS SUBSCRIBER NOT CONNECTED.");
			return -1;
		}

		try
		{
			for (auto& ch : channels) {
				sub->subscribe(ch);
			}
		}
		catch (const Error& e)
		{
			redis_set_last_error(e.what());
			MF_Log("[Redis] subscribe failed: %s", e.what());
			return -1;
		}
		catch (const std::exception& e)
		{
			redis_set_last_error(e.what());
			MF_Log("[Redis] subscribe failed: %s", e.what());
			return -1;
		}

		th_subscriber = new std::thread(consumeThread);
	}
	else 
	{
		MF_Log("[WARN] NO REGISTED CHANNELS.");
	}

	return 0;
}

