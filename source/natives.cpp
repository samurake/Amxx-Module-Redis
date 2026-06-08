#include "module.h"

static cell	Native_RedisConnect(AMX *amx, cell *params)     {return redis_connect(amx, params);}

static cell Native_RedisDeleteKey(AMX *amx, cell *params)   {return redis_delete_key(amx, params);}
static cell Native_RedisHDeleteField(AMX *amx, cell *params){return redis_hdelete_field(amx, params);}

static cell Native_RedisGetString(AMX *amx, cell *params)   {return redis_get_string(amx, params);}
static cell Native_RedisGetInteger(AMX *amx, cell *params)  {return redis_get_integer(amx, params);}

static cell Native_RedisSetString(AMX *amx, cell *params)   {return redis_set_string(amx, params);}
static cell Native_RedisSetInteger(AMX *amx, cell *params)  {return redis_set_integer(amx, params);}


static cell Native_RedisHGetString(AMX *amx, cell *params)  {return redis_hget_string(amx, params);}
static cell Native_RedisHGetInteger(AMX *amx, cell *params) {return redis_hget_integer(amx, params);}

static cell Native_RedisHSetString(AMX *amx, cell *params)  {return redis_hset_string(amx, params);}
static cell Native_RedisHSetInteger(AMX *amx, cell *params) {return redis_hset_integer(amx, params);}

static cell Native_RedisPublish(AMX* amx, cell* params)		{return redis_publish(amx, params); }
static cell Native_RedisAsyncPublish(AMX* amx, cell* params) {return redis_async_publish(amx, params); }
static cell Native_RedisAsyncHSetString(AMX* amx, cell* params) {return redis_async_hset_string(amx, params); }
static cell Native_RedisAsyncHSetInteger(AMX* amx, cell* params) {return redis_async_hset_integer(amx, params); }
static cell Native_RedisAsyncSetString(AMX* amx, cell* params) {return redis_async_set_string(amx, params); }
static cell Native_RedisAsyncSetInteger(AMX* amx, cell* params) {return redis_async_set_integer(amx, params); }
static cell Native_RedisAsyncDeleteKey(AMX* amx, cell* params) {return redis_async_del_key(amx, params); }
static cell Native_RedisAsyncHDeleteField(AMX* amx, cell* params) {return redis_async_hdel_field(amx, params); }
static cell Native_RedisAsyncGetString(AMX* amx, cell* params) {return redis_async_get_string(amx, params); }
static cell Native_RedisAsyncGetInteger(AMX* amx, cell* params) {return redis_async_get_integer(amx, params); }
static cell Native_RedisAsyncHGetString(AMX* amx, cell* params) {return redis_async_hget_string(amx, params); }
static cell Native_RedisAsyncHGetInteger(AMX* amx, cell* params) {return redis_async_hget_integer(amx, params); }
static cell Native_RedisAsyncQueueSize(AMX* amx, cell* params) {return redis_async_queue_size(amx, params); }
static cell Native_RedisAsyncSetQueueLimit(AMX* amx, cell* params) {return redis_async_set_queue_limit(amx, params); }
static cell Native_RedisAsyncLastError(AMX* amx, cell* params) {return redis_async_last_error(amx, params); }

static cell Native_RedisRegisterSubscriber(AMX* amx, cell* params)	{return redis_register_subscriber(amx, params); }

AMX_NATIVE_INFO g_natives[] =
{
	{"redis_connect", Native_RedisConnect},
	{"redis_set_string", Native_RedisSetString},
	{"redis_set_integer", Native_RedisSetInteger},
	{"redis_get_string", Native_RedisGetString},
	{"redis_get_integer", Native_RedisGetInteger},
	{"redis_hset_string", Native_RedisHSetString},
	{"redis_hset_integer", Native_RedisHSetInteger},
	{"redis_hget_string", Native_RedisHGetString},
	{"redis_hget_integer", Native_RedisHGetInteger},
	{"redis_del_key", Native_RedisDeleteKey},
	{"redis_hdel_field", Native_RedisHDeleteField},

	{"redis_publish", Native_RedisPublish},
	{"redis_async_publish", Native_RedisAsyncPublish},
	{"redis_async_hset_string", Native_RedisAsyncHSetString},
	{"redis_async_hset_integer", Native_RedisAsyncHSetInteger},
	{"redis_async_set_string", Native_RedisAsyncSetString},
	{"redis_async_set_integer", Native_RedisAsyncSetInteger},
	{"redis_async_del_key", Native_RedisAsyncDeleteKey},
	{"redis_async_hdel_field", Native_RedisAsyncHDeleteField},
	{"redis_async_get_string", Native_RedisAsyncGetString},
	{"redis_async_get_integer", Native_RedisAsyncGetInteger},
	{"redis_async_hget_string", Native_RedisAsyncHGetString},
	{"redis_async_hget_integer", Native_RedisAsyncHGetInteger},
	{"redis_async_queue_size", Native_RedisAsyncQueueSize},
	{"redis_async_set_queue_limit", Native_RedisAsyncSetQueueLimit},
	{"redis_async_last_error", Native_RedisAsyncLastError},

	{"redis_register_subscriber", Native_RedisRegisterSubscriber },

	{nullptr, nullptr}
};
