#ifndef MODULE_H
#define MODULE_H

#include "amxxsdk/amxxmodule.h"
#include "sw/redis++/redis++.h"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <exception>
#include <map>
#include <memory>
#include <mutex>
#include <limits>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>

#define DEBUG_LOGGING 0

using namespace sw::redis;

extern AMX_NATIVE_INFO g_natives[];
extern Redis* g_redis;
extern Redis* g_subscriber_redis;
extern ConnectionOptions g_connection_options;

extern bool isSubscriberRunning;

extern int ForwardRedisOnMessage;
extern int ForwardRedisAsyncOnResult;
extern int ForwardRedisAsyncOnConnect;
extern int ForwardRedisAsyncOnResultEx;
extern int ForwardRedisAsyncOnConnection;
extern int HasRedisOnMessage;
extern bool RedisSubscriberForwardRegistered;
extern bool RedisAsyncForwardsRegistered;

extern std::vector<std::string> channels;
extern std::thread* th_subscriber;
extern sw::redis::Subscriber* sub;

extern const char* convertToCString(const OptionalString& optStr);
extern void redis_set_last_error(const char* message);
extern cell	redis_connect(AMX *amx, cell *params);
extern cell redis_last_error(AMX *amx, cell *params);

extern cell redis_delete_key(AMX *amx, cell *params);
extern cell redis_hdelete_field(AMX *amx, cell *params);

extern cell redis_get_string(AMX *amx, cell *params);
extern cell redis_get_integer(AMX *amx, cell *params);

extern cell redis_set_string(AMX *amx, cell *params);
extern cell redis_set_integer(AMX *amx, cell *params);

extern cell redis_hget_string(AMX *amx, cell *params);
extern cell redis_hget_integer(AMX *amx, cell *params);

extern cell redis_hset_string(AMX *amx, cell *params);
extern cell redis_hset_integer(AMX *amx, cell *params);

extern cell redis_publish(AMX* amx, cell* params);
extern cell redis_async_connect(AMX* amx, cell* params);
extern cell redis_async_open(AMX* amx, cell* params);
extern cell redis_async_close(AMX* amx, cell* params);
extern cell redis_async_status(AMX* amx, cell* params);
extern cell redis_async_publish(AMX* amx, cell* params);
extern cell redis_async_xadd(AMX* amx, cell* params);
extern cell redis_async_hset_string(AMX* amx, cell* params);
extern cell redis_async_hset_integer(AMX* amx, cell* params);
extern cell redis_async_set_string(AMX* amx, cell* params);
extern cell redis_async_set_integer(AMX* amx, cell* params);
extern cell redis_async_del_key(AMX* amx, cell* params);
extern cell redis_async_hdel_field(AMX* amx, cell* params);
extern cell redis_async_queue_size(AMX* amx, cell* params);
extern cell redis_async_queue_bytes(AMX* amx, cell* params);
extern cell redis_async_dropped_results(AMX* amx, cell* params);
extern cell redis_async_set_queue_limit(AMX* amx, cell* params);
extern cell redis_async_last_error(AMX* amx, cell* params);
extern cell redis_async_last_error_on(AMX* amx, cell* params);
extern cell redis_async_get_string(AMX* amx, cell* params);
extern cell redis_async_get_integer(AMX* amx, cell* params);
extern cell redis_async_hget_string(AMX* amx, cell* params);
extern cell redis_async_hget_integer(AMX* amx, cell* params);
extern cell redis_async_publish_on(AMX* amx, cell* params);
extern cell redis_async_xadd_on(AMX* amx, cell* params);
extern cell redis_async_hset_string_on(AMX* amx, cell* params);
extern cell redis_async_hset_integer_on(AMX* amx, cell* params);
extern cell redis_async_set_string_on(AMX* amx, cell* params);
extern cell redis_async_set_integer_on(AMX* amx, cell* params);
extern cell redis_async_del_key_on(AMX* amx, cell* params);
extern cell redis_async_hdel_field_on(AMX* amx, cell* params);
extern cell redis_async_get_string_on(AMX* amx, cell* params);
extern cell redis_async_get_integer_on(AMX* amx, cell* params);
extern cell redis_async_hget_string_on(AMX* amx, cell* params);
extern cell redis_async_hget_integer_on(AMX* amx, cell* params);
extern cell redis_async_queue_size_on(AMX* amx, cell* params);
extern cell redis_async_queue_bytes_on(AMX* amx, cell* params);

extern cell redis_register_subscriber(AMX* amx, cell* params);
extern cell redis_start_subscribe(AMX* amx, cell* params);

extern cell redis_start_subscribe_now(bool hasOnMessage);
extern cell redis_register_subscriber_forward(bool hasOnMessage);
extern void redis_start_async_worker();
extern bool redis_start_async_worker(const ConnectionOptions& options, int request_id);
extern void redis_stop_async_worker();
extern void redis_register_async_forwards();
extern void redis_dispatch_async_results();

extern bool UTIL_CheckForPublic(const char* publicname);

#endif // MODULE_H
