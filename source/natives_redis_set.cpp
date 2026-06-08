#include "module.h"

using namespace sw::redis;

// native redis_set_string(const key[], const value[], const ttl = 0, const type = 0, const keepttl = 0);
cell redis_set_string(AMX *amx, cell *params)
{
    int len = 0;
    
	std::string key   = MF_GetAmxString(amx, params[1], 0, &len);
    std::string value = MF_GetAmxString(amx, params[2], 1, &len);

    int ttl = params[3];
    UpdateType type = (UpdateType)params[4];
    bool keepttl = (params[5] != 0);

    if (g_redis != nullptr)
    {
        try
        {
            bool success = false;

            if (keepttl)
            {
                success = g_redis->set(key, value, keepttl, type);
            }
            else
            {
                success = g_redis->set(key, value, std::chrono::seconds(ttl), type);
            }

            return success ? 0 : -1;
        }
        catch (const Error& e)
        {
            redis_set_last_error(e.what());
            return -1;
        }
        catch (const std::exception& e)
        {
            redis_set_last_error(e.what());
            return -1;
        }
        catch (...)
        {
            redis_set_last_error("unknown Redis set error");
            return -1;
        }
    }

    return -1;
}


// native redis_set_integer(const key[], const value[], const ttl = 0, const type = 0, const keepttl = 0);
cell redis_set_integer(AMX *amx, cell *params)
{
    int len = 0;

	std::string key = MF_GetAmxString(amx, params[1], 0, &len);
    int value = params[2];

    int ttl = params[3];
    UpdateType type = (UpdateType)params[4];
    bool keepttl = (params[5] != 0);

    if (g_redis != nullptr)
    {
        try
        {
            bool success = false;

            if (keepttl)
            {
                success = g_redis->set(key, std::to_string(value), keepttl, type);
            }
            else
            {
                success = g_redis->set(key, std::to_string(value), std::chrono::seconds(ttl), type);
            }

            return success ? 0 : -1;
        }
        catch (const Error& e)
        {
            redis_set_last_error(e.what());
            return -1;
        }
        catch (const std::exception& e)
        {
            redis_set_last_error(e.what());
            return -1;
        }
        catch (...)
        {
            redis_set_last_error("unknown Redis set error");
            return -1;
        }
    }

    return -1;
}
