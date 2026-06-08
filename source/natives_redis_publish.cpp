#include "module.h"

using namespace sw::redis;

// native redis_publish(const channel[], const message[]);
cell redis_publish(AMX *amx, cell *params)
{
	int len = 0;
	std::string channel   = MF_GetAmxString(amx, params[1], 0, &len);
	std::string message	  = MF_GetAmxString(amx, params[2], 1, &len);

	if (g_redis == NULL)
		return -1;

	try
	{
		g_redis->publish(channel, message);
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
		redis_set_last_error("unknown Redis publish error");
		return -1;
	}

	return 0;
}
