#include "module.h"

using namespace sw::redis;

Redis* g_redis = NULL;
Redis* g_subscriber_redis = NULL;

ConnectionOptions g_connection_options;
sw::redis::Subscriber *sub;
std::string g_redis_last_error;
std::mutex g_redis_last_error_mutex;

const char* convertToCString(const OptionalString& optStr) {
    if (optStr) {
        return optStr->c_str();
    } else {
        return nullptr;
    }
}

void redis_set_last_error(const char* message)
{
    std::lock_guard<std::mutex> lock(g_redis_last_error_mutex);
    g_redis_last_error = message ? message : "";
}

// native redis_last_error(output[], maxlength);
cell redis_last_error(AMX *amx, cell *params)
{
    std::lock_guard<std::mutex> lock(g_redis_last_error_mutex);
    MF_SetAmxString(amx, params[1], g_redis_last_error.c_str(), params[2]);
    return static_cast<cell>(g_redis_last_error.length());
}

// native redis_connect(const hostip[], const port, const username[] = "", const password[] = "");
cell redis_connect(AMX *amx, cell *params)
{
	int len = 0;
	// HOST IP.
	g_connection_options.host = MF_GetAmxString(amx, params[1], 0, &len);
	// PORT
	g_connection_options.port = params[2];
    g_connection_options.connect_timeout = std::chrono::milliseconds(1000);
    g_connection_options.socket_timeout = std::chrono::milliseconds(1000);

	std::string username = MF_GetAmxString(amx, params[3], 1, &len);
    std::string password = MF_GetAmxString(amx, params[4], 2, &len);

	if (!username.empty())
	{
		// USERNAME
		g_connection_options.user = username;
	}
    else
    {
        g_connection_options.user.clear();
    }

    if (!password.empty())
    {
        // PASSWORD
        g_connection_options.password = password;
    }
    else
    {
        g_connection_options.password.clear();
    }

	try 
    {
        if (g_redis)
        {
            redis_stop_async_worker();
            delete g_redis;
            g_redis = nullptr;
        }

        g_redis = new Redis(g_connection_options);
        g_redis->ping();
        redis_start_async_worker();
        redis_set_last_error("");

    } catch (const Error &e) {
        redis_set_last_error(e.what());
        MF_Log("[Redis] connection failed: %s", e.what());
        return -1;
    } catch (const std::exception &e) {
        redis_set_last_error(e.what());
        MF_Log("[Redis] connection failed: %s", e.what());
        return -1;
    } catch (...) {
        redis_set_last_error("unknown Redis connection error");
        MF_Log("[Redis] connection failed: unknown Redis connection error");
        return -1;
    }
    return 0;
}

