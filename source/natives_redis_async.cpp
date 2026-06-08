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
        HDel
    };

    struct AsyncCommand
    {
        AsyncCommandType type;
        std::string key;
        std::string field;
        std::string value;
        int ttl = 0;
        UpdateType update_type = static_cast<UpdateType>(0);
        bool keepttl = false;
    };

    std::deque<AsyncCommand> g_async_queue;
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

    bool enqueue_async_command(const AsyncCommand& command)
    {
        if (!g_async_running.load())
        {
            return false;
        }

        {
            std::lock_guard<std::mutex> lock(g_async_mutex);
            if (g_async_queue.size() >= g_async_queue_limit)
            {
                g_async_last_error = "async queue full";
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
        }
    }

    void async_worker_main(ConnectionOptions options)
    {
        try
        {
            Redis redis(options);

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
                    set_async_error(e.what());
                }
                catch (const std::exception& e)
                {
                    set_async_error(e.what());
                }
            }
        }
        catch (const Error& e)
        {
            set_async_error(e.what());
        }
        catch (const std::exception& e)
        {
            set_async_error(e.what());
        }
    }
}

void redis_start_async_worker()
{
    if (g_async_running.load())
    {
        return;
    }

    g_async_running.store(true);
    g_async_worker = new std::thread(async_worker_main, g_connection_options);
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
