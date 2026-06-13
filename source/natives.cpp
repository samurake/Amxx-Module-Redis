#include "module.h"

static cell	Native_RedisConnect(AMX *amx, cell *params)     {return redis_connect(amx, params);}
static cell Native_RedisLastError(AMX *amx, cell *params)  {return redis_last_error(amx, params);}

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
static cell Native_RedisAsyncConnect(AMX* amx, cell* params) {return redis_async_connect(amx, params); }
static cell Native_RedisAsyncOpen(AMX* amx, cell* params) {return redis_async_open(amx, params); }
static cell Native_RedisAsyncClose(AMX* amx, cell* params) {return redis_async_close(amx, params); }
static cell Native_RedisAsyncStatus(AMX* amx, cell* params) {return redis_async_status(amx, params); }
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
static cell Native_RedisAsyncLastErrorOn(AMX* amx, cell* params) {return redis_async_last_error_on(amx, params); }
static cell Native_RedisAsyncPublishOn(AMX* amx, cell* params) {return redis_async_publish_on(amx, params); }
static cell Native_RedisAsyncHSetStringOn(AMX* amx, cell* params) {return redis_async_hset_string_on(amx, params); }
static cell Native_RedisAsyncHSetIntegerOn(AMX* amx, cell* params) {return redis_async_hset_integer_on(amx, params); }
static cell Native_RedisAsyncSetStringOn(AMX* amx, cell* params) {return redis_async_set_string_on(amx, params); }
static cell Native_RedisAsyncSetIntegerOn(AMX* amx, cell* params) {return redis_async_set_integer_on(amx, params); }
static cell Native_RedisAsyncDeleteKeyOn(AMX* amx, cell* params) {return redis_async_del_key_on(amx, params); }
static cell Native_RedisAsyncHDeleteFieldOn(AMX* amx, cell* params) {return redis_async_hdel_field_on(amx, params); }
static cell Native_RedisAsyncGetStringOn(AMX* amx, cell* params) {return redis_async_get_string_on(amx, params); }
static cell Native_RedisAsyncGetIntegerOn(AMX* amx, cell* params) {return redis_async_get_integer_on(amx, params); }
static cell Native_RedisAsyncHGetStringOn(AMX* amx, cell* params) {return redis_async_hget_string_on(amx, params); }
static cell Native_RedisAsyncHGetIntegerOn(AMX* amx, cell* params) {return redis_async_hget_integer_on(amx, params); }
static cell Native_RedisAsyncQueueSizeOn(AMX* amx, cell* params) {return redis_async_queue_size_on(amx, params); }

static cell Native_RedisRegisterSubscriber(AMX* amx, cell* params)	{return redis_register_subscriber(amx, params); }
static cell Native_RedisStartSubscribe(AMX* amx, cell* params) {return redis_start_subscribe(amx, params); }

AMX_NATIVE_INFO g_natives[] =
{
	{"redis_connect", Native_RedisConnect},
	{"redis_last_error", Native_RedisLastError},
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
	{"redis_async_connect", Native_RedisAsyncConnect},
	{"redis_async_open", Native_RedisAsyncOpen},
	{"redis_async_close", Native_RedisAsyncClose},
	{"redis_async_status", Native_RedisAsyncStatus},
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
	{"redis_async_last_error_on", Native_RedisAsyncLastErrorOn},
	{"redis_async_publish_on", Native_RedisAsyncPublishOn},
	{"redis_async_hset_string_on", Native_RedisAsyncHSetStringOn},
	{"redis_async_hset_integer_on", Native_RedisAsyncHSetIntegerOn},
	{"redis_async_set_string_on", Native_RedisAsyncSetStringOn},
	{"redis_async_set_integer_on", Native_RedisAsyncSetIntegerOn},
	{"redis_async_del_key_on", Native_RedisAsyncDeleteKeyOn},
	{"redis_async_hdel_field_on", Native_RedisAsyncHDeleteFieldOn},
	{"redis_async_get_string_on", Native_RedisAsyncGetStringOn},
	{"redis_async_get_integer_on", Native_RedisAsyncGetIntegerOn},
	{"redis_async_hget_string_on", Native_RedisAsyncHGetStringOn},
	{"redis_async_hget_integer_on", Native_RedisAsyncHGetIntegerOn},
	{"redis_async_queue_size_on", Native_RedisAsyncQueueSizeOn},

	{"redis_register_subscriber", Native_RedisRegisterSubscriber },
	{"redis_start_subscribe", Native_RedisStartSubscribe },

	{nullptr, nullptr}
};
