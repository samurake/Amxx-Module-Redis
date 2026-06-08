#include "module.h"

using namespace sw::redis;

// native redis_hget_string(const key[], const field[], value[], maxlength);
cell redis_hget_string(AMX *amx, cell *params)
{
	int len = 0;
	std::string key   = MF_GetAmxString(amx, params[1], 0, &len);
	std::string field = MF_GetAmxString(amx, params[2], 1, &len);

	if (g_redis != NULL)
	{
		try
		{
			OptionalString value = g_redis->hget(key, field);
			const char *result = convertToCString(value);
			MF_SetAmxString(amx, params[3], result ? result : "", params[4]);
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
			redis_set_last_error("unknown Redis hget error");
			return -1;
		}
	}
	else
		return -1;

	return 0;
}

// native redis_hget_string(const key[], const field[], value);
cell redis_hget_integer(AMX *amx, cell *params)
{
	int len = 0;
	std::string key = MF_GetAmxString(amx, params[1], 0, &len);
	std::string field = MF_GetAmxString(amx, params[2], 1, &len);
	int iResult = 0;
	if (g_redis != NULL)
	{
		try
		{
			OptionalString value = g_redis->hget(key, field);
			if (!value)
			{
				return 0;
			}

			std::string result = value.value();
			iResult = std::stoi(result);
		}
		catch (const Error& e)
		{
			redis_set_last_error(e.what());
			iResult = 0;
			return 0;
		}
		catch (const std::exception& e)
		{
			redis_set_last_error(e.what());
			iResult = 0;
			return 0;
		}
		catch (...)
		{
			redis_set_last_error("unknown Redis integer hget error");
			iResult = 0;
			return 0;
		}
	}
	else
		return 0;

	return iResult;
}
