#include "module.h"

using namespace sw::redis;

std::vector<std::string> channels;
std::thread *th_subscriber = NULL;
bool isSubscriberRunning = false;
ConnectionOptions g_subscriber_options;

namespace
{
	bool channel_registered(const std::string& channel)
	{
		for (const auto& registered : channels)
		{
			if (registered == channel)
			{
				return true;
			}
		}

		return false;
	}
}

cell redis_register_subscriber_forward(bool hasOnMessage)
{
	if (hasOnMessage)
	{
		if (sub != nullptr)
		{
			return 0;
		}

		try
		{
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
	int len = 0;
	std::string channel = MF_GetAmxString(amx, params[1], 0, &len);

	if (channel.empty())
	{
		redis_set_last_error("Redis subscriber channel is empty");
		return -1;
	}

	if (g_redis != NULL)
	{
		if (!channel_registered(channel))
		{
			channels.push_back(channel);
		}
	}
	else
	{
		redis_set_last_error("Redis is not connected; call redis_connect before redis_register_subscriber");
		return -1;
	}

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
			redis_set_last_error(err.what());
			MF_Log("[Redis] subscribe consume failed: %s", err.what());
			isSubscriberRunning = false;
			return;
		}
		catch (const std::exception& err)
		{
			redis_set_last_error(err.what());
			MF_Log("[Redis] subscribe consume failed: %s", err.what());
			isSubscriberRunning = false;
			return;
		}
		catch (...)
		{
			redis_set_last_error("unknown Redis subscribe consume error");
			MF_Log("[Redis] subscribe consume failed: unknown Redis subscribe consume error");
			isSubscriberRunning = false;
			return;
		}
	}
}

cell redis_start_subscribe(AMX* amx, cell* params)
{
	if (HasRedisOnMessage < 0)
	{
		return 0;
	}

	return redis_start_subscribe_now(HasRedisOnMessage);
}

// native redis_start_subscribe();
cell redis_start_subscribe_now(bool hasOnMessage)
{
	if (isSubscriberRunning)
	{
		return 0;
	}

	if (!hasOnMessage)
	{
		redis_set_last_error("Redis_Subscriber_OnMessage forward does not exist");
		MF_Log("[Redis] Redis_Subscriber_OnMessage forward does not exist.");
		return -1;
	}

	if (channels.size() > 0)
	{
		if (sub == nullptr)
		{
			if (redis_register_subscriber_forward(hasOnMessage) != 0)
			{
				return -1;
			}
		}

		if (sub == nullptr)
		{
			redis_set_last_error("Redis subscriber is not connected");
			MF_Log("[Redis] subscriber is not connected.");
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
		catch (...)
		{
			redis_set_last_error("unknown Redis subscribe error");
			MF_Log("[Redis] subscribe failed: unknown Redis subscribe error");
			return -1;
		}

		try
		{
			th_subscriber = new std::thread(consumeThread);
		}
		catch (const std::exception& e)
		{
			redis_set_last_error(e.what());
			MF_Log("[Redis] subscriber thread start failed: %s", e.what());
			return -1;
		}
		catch (...)
		{
			redis_set_last_error("unknown Redis subscriber thread start error");
			MF_Log("[Redis] subscriber thread start failed: unknown Redis subscriber thread start error");
			return -1;
		}
	}
	else 
	{
		redis_set_last_error("no Redis subscriber channels registered");
		MF_Log("[Redis] no subscriber channels registered.");
		return -1;
	}

	return 0;
}

