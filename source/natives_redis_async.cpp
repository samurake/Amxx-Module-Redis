#include "module.h"

using namespace sw::redis;

namespace
{
    enum class AsyncCommandType
    {
        Publish,
        HSet,
        Set,
        Del,
        HDel,
        Get,
        HGet
    };

    struct AsyncCommand
    {
        AsyncCommandType type;
        int request_id = 0;
        std::string key;
        std::string field;
        std::string value;
        int ttl = 0;
        UpdateType update_type = static_cast<UpdateType>(0);
        bool keepttl = false;
    };

    struct AsyncResult
    {
        int request_id = 0;
        std::string command;
        int status = -1;
        std::string key;
        std::string field;
        std::string value;
    };

    struct AsyncConnectResult
    {
        int request_id = 0;
        int status = -1;
        std::string error;
    };

    std::deque<AsyncCommand> g_async_queue;
    std::deque<AsyncResult> g_async_results;
    std::deque<AsyncConnectResult> g_async_connect_results;
    std::mutex g_async_mutex;
    std::condition_variable g_async_cv;
    std::thread* g_async_worker = nullptr;
    std::atomic<bool> g_async_running(false);
    size_t g_async_queue_limit = 4096;
    std::string g_async_last_error;

    void set_async_error(const std::string& message)
    {
        std::lock_guard<std::mutex> lock(g_async_mutex);
        g_async_last_error = message;
    }

    void record_async_error(const std::string& message)
    {
        set_async_error(message);
        redis_set_last_error(message.c_str());
    }

    void enqueue_async_result(const AsyncResult& result)
    {
        std::lock_guard<std::mutex> lock(g_async_mutex);
        g_async_results.push_back(result);
    }

    void enqueue_async_connect_result(int request_id, int status, const std::string& error)
    {
        AsyncConnectResult result;
        result.request_id = request_id;
        result.status = status;
        result.error = error;

        std::lock_guard<std::mutex> lock(g_async_mutex);
        g_async_connect_results.push_back(result);
    }

    void enqueue_async_error_result(const AsyncCommand& command, const std::string& message)
    {
        if (command.type != AsyncCommandType::Get && command.type != AsyncCommandType::HGet)
        {
            return;
        }

        AsyncResult result;
        result.request_id = command.request_id;
        result.command = command.type == AsyncCommandType::Get ? "get" : "hget";
        result.status = -1;
        result.key = command.key;
        result.field = command.field;
        result.value = message;
        enqueue_async_result(result);
    }

    void fail_queued_async_commands(const std::string& message)
    {
        std::deque<AsyncCommand> failed_commands;

        {
            std::lock_guard<std::mutex> lock(g_async_mutex);
            failed_commands.swap(g_async_queue);
        }

        for (const auto& command : failed_commands)
        {
            enqueue_async_error_result(command, message);
        }
    }

    bool enqueue_async_command(const AsyncCommand& command)
    {
        if (!g_async_running.load())
        {
            record_async_error("async worker is not running; call redis_async_connect first");
            return false;
        }

        {
            std::lock_guard<std::mutex> lock(g_async_mutex);
            if (g_async_queue.size() >= g_async_queue_limit)
            {
                g_async_last_error = "async queue full";
                redis_set_last_error("async queue full");
                return false;
            }

            g_async_queue.push_back(command);
        }

        g_async_cv.notify_one();
        return true;
    }

    void execute_async_command(Redis& redis, const AsyncCommand& command)
    {
        switch (command.type)
        {
            case AsyncCommandType::Publish:
                redis.publish(command.key, command.value);
                break;

            case AsyncCommandType::HSet:
                redis.hset(command.key, command.field, command.value);
                break;

            case AsyncCommandType::Set:
                if (command.keepttl)
                {
                    redis.set(command.key, command.value, command.keepttl, command.update_type);
                }
                else
                {
                    redis.set(command.key, command.value, std::chrono::seconds(command.ttl), command.update_type);
                }
                break;

            case AsyncCommandType::Del:
                redis.del(command.key);
                break;

            case AsyncCommandType::HDel:
                redis.hdel(command.key, command.field);
                break;

            case AsyncCommandType::Get:
            {
                AsyncResult result;
                result.request_id = command.request_id;
                result.command = "get";
                result.key = command.key;

                OptionalString value = redis.get(command.key);
                if (value)
                {
                    result.status = 0;
                    result.value = *value;
                }
                else
                {
                    result.status = 1;
                }

                enqueue_async_result(result);
                break;
            }

            case AsyncCommandType::HGet:
            {
                AsyncResult result;
                result.request_id = command.request_id;
                result.command = "hget";
                result.key = command.key;
                result.field = command.field;

                OptionalString value = redis.hget(command.key, command.field);
                if (value)
                {
                    result.status = 0;
                    result.value = *value;
                }
                else
                {
                    result.status = 1;
                }

                enqueue_async_result(result);
                break;
            }
        }
    }

    void async_worker_main(ConnectionOptions options, int connect_request_id)
    {
        try
        {
            Redis redis(options);
            redis.ping();
            enqueue_async_connect_result(connect_request_id, 0, "");

            while (true)
            {
                AsyncCommand command;

                {
                    std::unique_lock<std::mutex> lock(g_async_mutex);
                    g_async_cv.wait(lock, [] {
                        return !g_async_running.load() || !g_async_queue.empty();
                    });

                    if (!g_async_running.load() && g_async_queue.empty())
                    {
                        break;
                    }

                    command = g_async_queue.front();
                    g_async_queue.pop_front();
                }

                try
                {
                    execute_async_command(redis, command);
                }
                catch (const Error& e)
                {
                    record_async_error(e.what());
                    enqueue_async_error_result(command, e.what());
                }
                catch (const std::exception& e)
                {
                    record_async_error(e.what());
                    enqueue_async_error_result(command, e.what());
                }
                catch (...)
                {
                    record_async_error("unknown Redis async command error");
                    enqueue_async_error_result(command, "unknown Redis async command error");
                }
            }
        }
        catch (const Error& e)
        {
            record_async_error(e.what());
            enqueue_async_connect_result(connect_request_id, -1, e.what());
            fail_queued_async_commands(e.what());
            g_async_running.store(false);
        }
        catch (const std::exception& e)
        {
            record_async_error(e.what());
            enqueue_async_connect_result(connect_request_id, -1, e.what());
            fail_queued_async_commands(e.what());
            g_async_running.store(false);
        }
        catch (...)
        {
            record_async_error("unknown Redis async connection error");
            enqueue_async_connect_result(connect_request_id, -1, "unknown Redis async connection error");
            fail_queued_async_commands("unknown Redis async connection error");
            g_async_running.store(false);
        }
    }

    bool cleanup_async_worker_if_stopped()
    {
        if (g_async_running.load() || !g_async_worker)
        {
            return false;
        }

        if (g_async_worker->joinable())
        {
            g_async_worker->join();
        }

        delete g_async_worker;
        g_async_worker = nullptr;
        return true;
    }
}

void redis_start_async_worker()
{
    redis_start_async_worker(g_connection_options, 0);
}

bool redis_start_async_worker(const ConnectionOptions& options, int request_id)
{
    if (g_async_running.load())
    {
        record_async_error("async worker already running");
        enqueue_async_connect_result(request_id, -1, "async worker already running");
        return false;
    }

    cleanup_async_worker_if_stopped();

    g_async_running.store(true);
    try
    {
        g_async_worker = new std::thread(async_worker_main, options, request_id);
    }
    catch (const std::exception& e)
    {
        record_async_error(e.what());
        g_async_running.store(false);
        enqueue_async_connect_result(request_id, -1, e.what());
        return false;
    }
    catch (...)
    {
        record_async_error("unknown async worker start error");
        g_async_running.store(false);
        enqueue_async_connect_result(request_id, -1, "unknown async worker start error");
        return false;
    }
    return true;
}

void redis_stop_async_worker()
{
    if (!g_async_running.load() && !g_async_worker)
    {
        return;
    }

    g_async_running.store(false);
    g_async_cv.notify_all();

    if (g_async_worker)
    {
        if (g_async_worker->joinable())
        {
            g_async_worker->join();
        }

        delete g_async_worker;
        g_async_worker = nullptr;
    }

    std::lock_guard<std::mutex> lock(g_async_mutex);
    g_async_queue.clear();
    g_async_results.clear();
    g_async_connect_results.clear();
}

void redis_dispatch_async_results()
{
    if (ForwardRedisAsyncOnConnect >= 0)
    {
        for (int i = 0; i < 8; i++)
        {
            AsyncConnectResult result;

            {
                std::lock_guard<std::mutex> lock(g_async_mutex);
                if (g_async_connect_results.empty())
                {
                    break;
                }

                result = g_async_connect_results.front();
                g_async_connect_results.pop_front();
            }

            MF_ExecuteForward(
                ForwardRedisAsyncOnConnect,
                result.request_id,
                result.status,
                result.error.c_str()
            );
        }
    }

    if (ForwardRedisAsyncOnResult < 0)
    {
        return;
    }

    for (int i = 0; i < 64; i++)
    {
        AsyncResult result;

        {
            std::lock_guard<std::mutex> lock(g_async_mutex);
            if (g_async_results.empty())
            {
                return;
            }

            result = g_async_results.front();
            g_async_results.pop_front();
        }

        MF_ExecuteForward(
            ForwardRedisAsyncOnResult,
            result.request_id,
            result.command.c_str(),
            result.status,
            result.key.c_str(),
            result.field.c_str(),
            result.value.c_str()
        );
    }
}

// native redis_async_connect(const hostip[], const port = 6379, const username[] = "", const password[] = "", request_id = 0);
cell redis_async_connect(AMX* amx, cell* params)
{
    int len = 0;
    ConnectionOptions options;
    options.host = MF_GetAmxString(amx, params[1], 0, &len);
    options.port = params[2];
    options.connect_timeout = std::chrono::milliseconds(1000);
    options.socket_timeout = std::chrono::milliseconds(1000);

    if (options.host.empty())
    {
        record_async_error("Redis host is empty");
        enqueue_async_connect_result(params[5], -1, "Redis host is empty");
        return -1;
    }

    if (options.port <= 0 || options.port > 65535)
    {
        record_async_error("Redis port is invalid");
        enqueue_async_connect_result(params[5], -1, "Redis port is invalid");
        return -1;
    }

    std::string username = MF_GetAmxString(amx, params[3], 1, &len);
    std::string password = MF_GetAmxString(amx, params[4], 2, &len);

    if (!username.empty())
    {
        options.user = username;
    }

    if (!password.empty())
    {
        options.password = password;
    }

    g_connection_options = options;

    if (!redis_start_async_worker(options, params[5]))
    {
        return -1;
    }

    redis_set_last_error("");
    return 0;
}

// native redis_async_publish(const channel[], const message[]);
cell redis_async_publish(AMX* amx, cell* params)
{
    int len = 0;
    AsyncCommand command;
    command.type = AsyncCommandType::Publish;
    command.key = MF_GetAmxString(amx, params[1], 0, &len);
    command.value = MF_GetAmxString(amx, params[2], 1, &len);

    return enqueue_async_command(command) ? 0 : -1;
}

// native redis_async_hset_string(const key[], const field[], const value[]);
cell redis_async_hset_string(AMX* amx, cell* params)
{
    int len = 0;
    AsyncCommand command;
    command.type = AsyncCommandType::HSet;
    command.key = MF_GetAmxString(amx, params[1], 0, &len);
    command.field = MF_GetAmxString(amx, params[2], 1, &len);
    command.value = MF_GetAmxString(amx, params[3], 2, &len);

    return enqueue_async_command(command) ? 0 : -1;
}

// native redis_async_hset_integer(const key[], const field[], const value);
cell redis_async_hset_integer(AMX* amx, cell* params)
{
    int len = 0;
    AsyncCommand command;
    command.type = AsyncCommandType::HSet;
    command.key = MF_GetAmxString(amx, params[1], 0, &len);
    command.field = MF_GetAmxString(amx, params[2], 1, &len);
    command.value = std::to_string(params[3]);

    return enqueue_async_command(command) ? 0 : -1;
}

// native redis_async_set_string(const key[], const value[], const ttl = 0, const type = 0, const keepttl = 0);
cell redis_async_set_string(AMX* amx, cell* params)
{
    int len = 0;
    AsyncCommand command;
    command.type = AsyncCommandType::Set;
    command.key = MF_GetAmxString(amx, params[1], 0, &len);
    command.value = MF_GetAmxString(amx, params[2], 1, &len);
    command.ttl = params[3];
    command.update_type = static_cast<UpdateType>(params[4]);
    command.keepttl = params[5] != 0;

    return enqueue_async_command(command) ? 0 : -1;
}

// native redis_async_set_integer(const key[], const value, const ttl = 0, const type = 0, const keepttl = 0);
cell redis_async_set_integer(AMX* amx, cell* params)
{
    int len = 0;
    AsyncCommand command;
    command.type = AsyncCommandType::Set;
    command.key = MF_GetAmxString(amx, params[1], 0, &len);
    command.value = std::to_string(params[2]);
    command.ttl = params[3];
    command.update_type = static_cast<UpdateType>(params[4]);
    command.keepttl = params[5] != 0;

    return enqueue_async_command(command) ? 0 : -1;
}

// native redis_async_del_key(const key[]);
cell redis_async_del_key(AMX* amx, cell* params)
{
    int len = 0;
    AsyncCommand command;
    command.type = AsyncCommandType::Del;
    command.key = MF_GetAmxString(amx, params[1], 0, &len);

    return enqueue_async_command(command) ? 0 : -1;
}

// native redis_async_hdel_field(const key[], const field[]);
cell redis_async_hdel_field(AMX* amx, cell* params)
{
    int len = 0;
    AsyncCommand command;
    command.type = AsyncCommandType::HDel;
    command.key = MF_GetAmxString(amx, params[1], 0, &len);
    command.field = MF_GetAmxString(amx, params[2], 1, &len);

    return enqueue_async_command(command) ? 0 : -1;
}

// native redis_async_get_string(const key[], request_id = 0);
cell redis_async_get_string(AMX* amx, cell* params)
{
    int len = 0;
    AsyncCommand command;
    command.type = AsyncCommandType::Get;
    command.key = MF_GetAmxString(amx, params[1], 0, &len);
    command.request_id = params[2];

    return enqueue_async_command(command) ? 0 : -1;
}

// native redis_async_get_integer(const key[], request_id = 0);
cell redis_async_get_integer(AMX* amx, cell* params)
{
    return redis_async_get_string(amx, params);
}

// native redis_async_hget_string(const key[], const field[], request_id = 0);
cell redis_async_hget_string(AMX* amx, cell* params)
{
    int len = 0;
    AsyncCommand command;
    command.type = AsyncCommandType::HGet;
    command.key = MF_GetAmxString(amx, params[1], 0, &len);
    command.field = MF_GetAmxString(amx, params[2], 1, &len);
    command.request_id = params[3];

    return enqueue_async_command(command) ? 0 : -1;
}

// native redis_async_hget_integer(const key[], const field[], request_id = 0);
cell redis_async_hget_integer(AMX* amx, cell* params)
{
    return redis_async_hget_string(amx, params);
}

// native redis_async_queue_size();
cell redis_async_queue_size(AMX* amx, cell* params)
{
    std::lock_guard<std::mutex> lock(g_async_mutex);
    return static_cast<cell>(g_async_queue.size());
}

// native redis_async_set_queue_limit(limit);
cell redis_async_set_queue_limit(AMX* amx, cell* params)
{
    if (params[1] < 1)
    {
        return -1;
    }

    std::lock_guard<std::mutex> lock(g_async_mutex);
    g_async_queue_limit = static_cast<size_t>(params[1]);
    return 0;
}

// native redis_async_last_error(output[], maxlength);
cell redis_async_last_error(AMX* amx, cell* params)
{
    std::string error;

    {
        std::lock_guard<std::mutex> lock(g_async_mutex);
        error = g_async_last_error;
    }

    MF_SetAmxString(amx, params[1], error.c_str(), params[2]);
    return error.empty() ? -1 : 0;
}
