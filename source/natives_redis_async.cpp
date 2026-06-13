#include "module.h"

using namespace sw::redis;

namespace
{
    const int DEFAULT_CONNECTION_ID = 1;
    const int STATUS_STOPPED = -1;
    const int STATUS_CONNECTING = 1;
    const int STATUS_CONNECTED = 0;
    const int STATUS_RECONNECTING = 2;
    const int STATUS_CLOSING = 3;
    const int CONNECT_STATUS_CLOSED = -2;

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
        int connection_id = DEFAULT_CONNECTION_ID;
        int request_id = 0;
        std::string command;
        int status = -1;
        std::string key;
        std::string field;
        std::string value;
    };

    struct AsyncConnectResult
    {
        int connection_id = DEFAULT_CONNECTION_ID;
        int request_id = 0;
        int status = -1;
        std::string error;
    };

    struct AsyncConnection
    {
        int id = DEFAULT_CONNECTION_ID;
        std::string name;
        ConnectionOptions options;
        std::deque<AsyncCommand> queue;
        std::deque<int> pending_connect_requests;
        std::mutex mutex;
        std::condition_variable cv;
        std::thread* worker = nullptr;
        bool closing = false;
        int state = STATUS_STOPPED;
        size_t queue_limit = 4096;
        std::string last_error;
    };

    std::map<int, std::shared_ptr<AsyncConnection>> g_connections;
    std::mutex g_connections_mutex;
    std::deque<AsyncResult> g_async_results;
    std::deque<AsyncConnectResult> g_async_connect_results;
    std::mutex g_async_results_mutex;
    size_t g_async_queue_limit = 4096;
    std::string g_async_last_error;
    int g_next_connection_id = DEFAULT_CONNECTION_ID + 1;

    int get_param_count(cell* params)
    {
        return static_cast<int>(params[0] / sizeof(cell));
    }

    bool same_options(const ConnectionOptions& left, const ConnectionOptions& right)
    {
        return left.host == right.host
            && left.port == right.port
            && left.user == right.user
            && left.password == right.password;
    }

    void set_global_async_error(const std::string& message)
    {
        {
            std::lock_guard<std::mutex> lock(g_async_results_mutex);
            g_async_last_error = message;
        }
        redis_set_last_error(message.c_str());
    }

    void set_connection_error(const std::shared_ptr<AsyncConnection>& connection, const std::string& message)
    {
        {
            std::lock_guard<std::mutex> lock(connection->mutex);
            connection->last_error = message;
        }
        set_global_async_error(message);
    }

    void enqueue_async_result(const AsyncResult& result)
    {
        std::lock_guard<std::mutex> lock(g_async_results_mutex);
        g_async_results.push_back(result);
    }

    void enqueue_async_connect_result(int connection_id, int request_id, int status, const std::string& error)
    {
        AsyncConnectResult result;
        result.connection_id = connection_id;
        result.request_id = request_id;
        result.status = status;
        result.error = error;

        std::lock_guard<std::mutex> lock(g_async_results_mutex);
        g_async_connect_results.push_back(result);
    }

    void enqueue_async_error_result(int connection_id, const AsyncCommand& command, const std::string& message)
    {
        if (command.type != AsyncCommandType::Get && command.type != AsyncCommandType::HGet)
        {
            return;
        }

        AsyncResult result;
        result.connection_id = connection_id;
        result.request_id = command.request_id;
        result.command = command.type == AsyncCommandType::Get ? "get" : "hget";
        result.status = -1;
        result.key = command.key;
        result.field = command.field;
        result.value = message;
        enqueue_async_result(result);
    }

    void drain_pending_connect_results(const std::shared_ptr<AsyncConnection>& connection, int status, const std::string& error)
    {
        std::deque<int> requests;

        {
            std::lock_guard<std::mutex> lock(connection->mutex);
            requests.swap(connection->pending_connect_requests);
        }

        for (int request_id : requests)
        {
            enqueue_async_connect_result(connection->id, request_id, status, error);
        }
    }

    void set_connection_state(const std::shared_ptr<AsyncConnection>& connection, int state)
    {
        std::lock_guard<std::mutex> lock(connection->mutex);
        connection->state = state;
    }

    ConnectionOptions read_options(AMX* amx, cell* params, int host_param, int port_param, int username_param, int password_param)
    {
        int len = 0;
        ConnectionOptions options;
        options.host = MF_GetAmxString(amx, params[host_param], 0, &len);
        options.port = params[port_param];
        options.connect_timeout = std::chrono::milliseconds(1000);
        options.socket_timeout = std::chrono::milliseconds(1000);

        std::string username = MF_GetAmxString(amx, params[username_param], 1, &len);
        std::string password = MF_GetAmxString(amx, params[password_param], 2, &len);

        if (!username.empty())
        {
            options.user = username;
        }

        if (!password.empty())
        {
            options.password = password;
        }

        return options;
    }

    bool validate_options(const ConnectionOptions& options, std::string& error)
    {
        if (options.host.empty())
        {
            error = "Redis host is empty";
            return false;
        }

        if (options.port <= 0 || options.port > 65535)
        {
            error = "Redis port is invalid";
            return false;
        }

        return true;
    }

    void execute_async_command(Redis& redis, int connection_id, const AsyncCommand& command)
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
                result.connection_id = connection_id;
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
                result.connection_id = connection_id;
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

    bool wait_before_reconnect(const std::shared_ptr<AsyncConnection>& connection, int backoff_ms)
    {
        std::unique_lock<std::mutex> lock(connection->mutex);
        if (connection->closing)
        {
            return false;
        }

        connection->cv.wait_for(lock, std::chrono::milliseconds(backoff_ms), [&connection] {
            return connection->closing;
        });

        return !connection->closing;
    }

    void async_connection_worker(std::shared_ptr<AsyncConnection> connection)
    {
        int backoff_ms = 1000;
        bool reconnect_notice_pending = false;

        while (true)
        {
            {
                std::lock_guard<std::mutex> lock(connection->mutex);
                if (connection->closing)
                {
                    connection->state = STATUS_CLOSING;
                    break;
                }
                if (connection->state != STATUS_RECONNECTING)
                {
                    connection->state = STATUS_CONNECTING;
                }
            }

            try
            {
                Redis redis(connection->options);
                redis.ping();

                {
                    std::lock_guard<std::mutex> lock(connection->mutex);
                    connection->state = STATUS_CONNECTED;
                    connection->last_error.clear();
                }
                backoff_ms = 1000;
                drain_pending_connect_results(connection, 0, "");
                if (reconnect_notice_pending)
                {
                    enqueue_async_connect_result(connection->id, 0, 0, "");
                    reconnect_notice_pending = false;
                }

                while (true)
                {
                    AsyncCommand command;

                    {
                        std::unique_lock<std::mutex> lock(connection->mutex);
                        connection->cv.wait(lock, [&connection] {
                            return connection->closing || !connection->queue.empty();
                        });

                        if (connection->closing)
                        {
                            connection->state = STATUS_CLOSING;
                            return;
                        }

                        command = connection->queue.front();
                        connection->queue.pop_front();
                    }

                    try
                    {
                        execute_async_command(redis, connection->id, command);
                    }
                    catch (const Error& e)
                    {
                        set_connection_error(connection, e.what());
                        enqueue_async_error_result(connection->id, command, e.what());
                        enqueue_async_connect_result(connection->id, 0, -1, e.what());
                        reconnect_notice_pending = true;
                        set_connection_state(connection, STATUS_RECONNECTING);
                        break;
                    }
                    catch (const std::exception& e)
                    {
                        set_connection_error(connection, e.what());
                        enqueue_async_error_result(connection->id, command, e.what());
                        enqueue_async_connect_result(connection->id, 0, -1, e.what());
                        reconnect_notice_pending = true;
                        set_connection_state(connection, STATUS_RECONNECTING);
                        break;
                    }
                    catch (...)
                    {
                        set_connection_error(connection, "unknown Redis async command error");
                        enqueue_async_error_result(connection->id, command, "unknown Redis async command error");
                        enqueue_async_connect_result(connection->id, 0, -1, "unknown Redis async command error");
                        reconnect_notice_pending = true;
                        set_connection_state(connection, STATUS_RECONNECTING);
                        break;
                    }
                }
            }
            catch (const Error& e)
            {
                set_connection_error(connection, e.what());
                drain_pending_connect_results(connection, -1, e.what());
                if (reconnect_notice_pending)
                {
                    enqueue_async_connect_result(connection->id, 0, -1, e.what());
                }
                reconnect_notice_pending = true;
            }
            catch (const std::exception& e)
            {
                set_connection_error(connection, e.what());
                drain_pending_connect_results(connection, -1, e.what());
                if (reconnect_notice_pending)
                {
                    enqueue_async_connect_result(connection->id, 0, -1, e.what());
                }
                reconnect_notice_pending = true;
            }
            catch (...)
            {
                set_connection_error(connection, "unknown Redis async connection error");
                drain_pending_connect_results(connection, -1, "unknown Redis async connection error");
                if (reconnect_notice_pending)
                {
                    enqueue_async_connect_result(connection->id, 0, -1, "unknown Redis async connection error");
                }
                reconnect_notice_pending = true;
            }

            {
                std::lock_guard<std::mutex> lock(connection->mutex);
                if (connection->closing)
                {
                    connection->state = STATUS_CLOSING;
                    break;
                }
                connection->state = STATUS_RECONNECTING;
            }

            if (!wait_before_reconnect(connection, backoff_ms))
            {
                break;
            }

            if (backoff_ms < 30000)
            {
                backoff_ms *= 2;
                if (backoff_ms > 30000)
                {
                    backoff_ms = 30000;
                }
            }
        }

        drain_pending_connect_results(connection, CONNECT_STATUS_CLOSED, "async connection closed");
        set_connection_state(connection, STATUS_STOPPED);
    }

    std::shared_ptr<AsyncConnection> find_connection(int connection_id)
    {
        std::lock_guard<std::mutex> lock(g_connections_mutex);
        auto iter = g_connections.find(connection_id);
        if (iter == g_connections.end())
        {
            return nullptr;
        }

        return iter->second;
    }

    void start_worker(const std::shared_ptr<AsyncConnection>& connection)
    {
        connection->worker = new std::thread(async_connection_worker, connection);
    }

    int create_connection(const ConnectionOptions& options, int request_id, const std::string& name, bool use_default)
    {
        std::shared_ptr<AsyncConnection> connection(new AsyncConnection());
        connection->id = use_default ? DEFAULT_CONNECTION_ID : g_next_connection_id++;
        connection->name = name;
        connection->options = options;
        connection->queue_limit = g_async_queue_limit;
        connection->state = STATUS_CONNECTING;
        connection->pending_connect_requests.push_back(request_id);

        g_connections[connection->id] = connection;

        try
        {
            start_worker(connection);
        }
        catch (const std::exception& e)
        {
            g_connections.erase(connection->id);
            set_global_async_error(e.what());
            enqueue_async_connect_result(connection->id, request_id, -1, e.what());
            return -1;
        }
        catch (...)
        {
            g_connections.erase(connection->id);
            set_global_async_error("unknown async worker start error");
            enqueue_async_connect_result(connection->id, request_id, -1, "unknown async worker start error");
            return -1;
        }

        return connection->id;
    }

    int open_async_connection(const ConnectionOptions& options, int request_id, const std::string& name, bool use_default, bool legacy_return)
    {
        std::string error;
        if (!validate_options(options, error))
        {
            set_global_async_error(error);
            enqueue_async_connect_result(use_default ? DEFAULT_CONNECTION_ID : 0, request_id, -1, error);
            return -1;
        }

        std::lock_guard<std::mutex> lock(g_connections_mutex);

        if (use_default)
        {
            auto existing = g_connections.find(DEFAULT_CONNECTION_ID);
            if (existing != g_connections.end())
            {
                auto connection = existing->second;
                bool connected = false;

                {
                    std::lock_guard<std::mutex> connection_lock(connection->mutex);
                    if (!same_options(connection->options, options))
                    {
                        error = "default async connection already uses a different Redis endpoint; use redis_async_open for another endpoint";
                        connection->last_error = error;
                        set_global_async_error(error);
                        enqueue_async_connect_result(DEFAULT_CONNECTION_ID, request_id, -1, error);
                        return -1;
                    }

                    connected = connection->state == STATUS_CONNECTED;
                    if (!connected)
                    {
                        connection->pending_connect_requests.push_back(request_id);
                    }
                }

                if (connected)
                {
                    enqueue_async_connect_result(DEFAULT_CONNECTION_ID, request_id, 0, "");
                }

                return legacy_return ? 0 : DEFAULT_CONNECTION_ID;
            }
        }

        int connection_id = create_connection(options, request_id, name, use_default);
        if (connection_id < 0)
        {
            return -1;
        }

        if (use_default)
        {
            g_connection_options = options;
        }
        redis_set_last_error("");
        return legacy_return ? 0 : connection_id;
    }

    bool enqueue_async_command_on(int connection_id, const AsyncCommand& command)
    {
        auto connection = find_connection(connection_id);
        if (!connection)
        {
            set_global_async_error("async connection handle is invalid");
            return false;
        }

        {
            std::lock_guard<std::mutex> lock(connection->mutex);
            if (connection->closing || connection->state == STATUS_STOPPED)
            {
                connection->last_error = "async connection is closed";
                set_global_async_error("async connection is closed");
                return false;
            }

            if (connection->queue.size() >= connection->queue_limit)
            {
                connection->last_error = "async queue full";
                set_global_async_error("async queue full");
                return false;
            }

            connection->queue.push_back(command);
        }

        connection->cv.notify_one();
        return true;
    }

    void stop_connection(const std::shared_ptr<AsyncConnection>& connection)
    {
        {
            std::lock_guard<std::mutex> lock(connection->mutex);
            connection->closing = true;
            connection->state = STATUS_CLOSING;
        }
        connection->cv.notify_all();

        if (connection->worker)
        {
            if (connection->worker->joinable())
            {
                connection->worker->join();
            }

            delete connection->worker;
            connection->worker = nullptr;
        }
    }

    AsyncCommand make_publish_command(AMX* amx, cell* params, int channel_param, int message_param)
    {
        int len = 0;
        AsyncCommand command;
        command.type = AsyncCommandType::Publish;
        command.key = MF_GetAmxString(amx, params[channel_param], 0, &len);
        command.value = MF_GetAmxString(amx, params[message_param], 1, &len);
        return command;
    }

    AsyncCommand make_hset_string_command(AMX* amx, cell* params, int key_param, int field_param, int value_param)
    {
        int len = 0;
        AsyncCommand command;
        command.type = AsyncCommandType::HSet;
        command.key = MF_GetAmxString(amx, params[key_param], 0, &len);
        command.field = MF_GetAmxString(amx, params[field_param], 1, &len);
        command.value = MF_GetAmxString(amx, params[value_param], 2, &len);
        return command;
    }

    AsyncCommand make_hset_integer_command(AMX* amx, cell* params, int key_param, int field_param, int value_param)
    {
        int len = 0;
        AsyncCommand command;
        command.type = AsyncCommandType::HSet;
        command.key = MF_GetAmxString(amx, params[key_param], 0, &len);
        command.field = MF_GetAmxString(amx, params[field_param], 1, &len);
        command.value = std::to_string(params[value_param]);
        return command;
    }

    AsyncCommand make_set_string_command(AMX* amx, cell* params, int key_param, int value_param, int ttl_param, int type_param, int keepttl_param)
    {
        int len = 0;
        AsyncCommand command;
        command.type = AsyncCommandType::Set;
        command.key = MF_GetAmxString(amx, params[key_param], 0, &len);
        command.value = MF_GetAmxString(amx, params[value_param], 1, &len);
        command.ttl = params[ttl_param];
        command.update_type = static_cast<UpdateType>(params[type_param]);
        command.keepttl = params[keepttl_param] != 0;
        return command;
    }

    AsyncCommand make_set_integer_command(AMX* amx, cell* params, int key_param, int value_param, int ttl_param, int type_param, int keepttl_param)
    {
        int len = 0;
        AsyncCommand command;
        command.type = AsyncCommandType::Set;
        command.key = MF_GetAmxString(amx, params[key_param], 0, &len);
        command.value = std::to_string(params[value_param]);
        command.ttl = params[ttl_param];
        command.update_type = static_cast<UpdateType>(params[type_param]);
        command.keepttl = params[keepttl_param] != 0;
        return command;
    }

    AsyncCommand make_del_command(AMX* amx, cell* params, int key_param)
    {
        int len = 0;
        AsyncCommand command;
        command.type = AsyncCommandType::Del;
        command.key = MF_GetAmxString(amx, params[key_param], 0, &len);
        return command;
    }

    AsyncCommand make_hdel_command(AMX* amx, cell* params, int key_param, int field_param)
    {
        int len = 0;
        AsyncCommand command;
        command.type = AsyncCommandType::HDel;
        command.key = MF_GetAmxString(amx, params[key_param], 0, &len);
        command.field = MF_GetAmxString(amx, params[field_param], 1, &len);
        return command;
    }

    AsyncCommand make_get_command(AMX* amx, cell* params, int key_param, int request_param)
    {
        int len = 0;
        AsyncCommand command;
        command.type = AsyncCommandType::Get;
        command.key = MF_GetAmxString(amx, params[key_param], 0, &len);
        command.request_id = params[request_param];
        return command;
    }

    AsyncCommand make_hget_command(AMX* amx, cell* params, int key_param, int field_param, int request_param)
    {
        int len = 0;
        AsyncCommand command;
        command.type = AsyncCommandType::HGet;
        command.key = MF_GetAmxString(amx, params[key_param], 0, &len);
        command.field = MF_GetAmxString(amx, params[field_param], 1, &len);
        command.request_id = params[request_param];
        return command;
    }
}

void redis_start_async_worker()
{
    redis_start_async_worker(g_connection_options, 0);
}

bool redis_start_async_worker(const ConnectionOptions& options, int request_id)
{
    return open_async_connection(options, request_id, "", true, true) == 0;
}

void redis_stop_async_worker()
{
    std::map<int, std::shared_ptr<AsyncConnection>> connections;

    {
        std::lock_guard<std::mutex> lock(g_connections_mutex);
        connections.swap(g_connections);
    }

    for (auto& item : connections)
    {
        stop_connection(item.second);
    }

    std::lock_guard<std::mutex> lock(g_async_results_mutex);
    g_async_results.clear();
    g_async_connect_results.clear();
}

void redis_dispatch_async_results()
{
    redis_register_async_forwards();

    for (int i = 0; i < 32; i++)
    {
        AsyncConnectResult result;

        {
            std::lock_guard<std::mutex> lock(g_async_results_mutex);
            if (g_async_connect_results.empty())
            {
                break;
            }

            result = g_async_connect_results.front();
            g_async_connect_results.pop_front();
        }

        if (ForwardRedisAsyncOnConnection >= 0)
        {
            MF_ExecuteForward(
                ForwardRedisAsyncOnConnection,
                result.connection_id,
                result.request_id,
                result.status,
                result.error.c_str()
            );
        }

        if (result.connection_id == DEFAULT_CONNECTION_ID && ForwardRedisAsyncOnConnect >= 0)
        {
            MF_ExecuteForward(
                ForwardRedisAsyncOnConnect,
                result.request_id,
                result.status,
                result.error.c_str()
            );
        }
    }

    for (int i = 0; i < 128; i++)
    {
        AsyncResult result;

        {
            std::lock_guard<std::mutex> lock(g_async_results_mutex);
            if (g_async_results.empty())
            {
                return;
            }

            result = g_async_results.front();
            g_async_results.pop_front();
        }

        if (ForwardRedisAsyncOnResultEx >= 0)
        {
            MF_ExecuteForward(
                ForwardRedisAsyncOnResultEx,
                result.connection_id,
                result.request_id,
                result.command.c_str(),
                result.status,
                result.key.c_str(),
                result.field.c_str(),
                result.value.c_str()
            );
        }

        if (result.connection_id == DEFAULT_CONNECTION_ID && ForwardRedisAsyncOnResult >= 0)
        {
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
}

cell redis_async_connect(AMX* amx, cell* params)
{
    ConnectionOptions options = read_options(amx, params, 1, 2, 3, 4);
    return open_async_connection(options, params[5], "", true, true);
}

cell redis_async_open(AMX* amx, cell* params)
{
    ConnectionOptions options = read_options(amx, params, 1, 2, 3, 4);

    int len = 0;
    std::string name;
    if (get_param_count(params) >= 6)
    {
        name = MF_GetAmxString(amx, params[6], 3, &len);
    }

    return open_async_connection(options, params[5], name, false, false);
}

cell redis_async_close(AMX* amx, cell* params)
{
    int connection_id = params[1];
    std::shared_ptr<AsyncConnection> connection;

    {
        std::lock_guard<std::mutex> lock(g_connections_mutex);
        auto iter = g_connections.find(connection_id);
        if (iter == g_connections.end())
        {
            set_global_async_error("async connection handle is invalid");
            return -1;
        }

        connection = iter->second;
        g_connections.erase(iter);
    }

    stop_connection(connection);
    return 0;
}

cell redis_async_status(AMX* amx, cell* params)
{
    auto connection = find_connection(params[1]);
    if (!connection)
    {
        return STATUS_STOPPED;
    }

    std::lock_guard<std::mutex> lock(connection->mutex);
    return connection->state;
}

cell redis_async_publish(AMX* amx, cell* params)
{
    return redis_async_publish_on(amx, params);
}

cell redis_async_hset_string(AMX* amx, cell* params)
{
    return enqueue_async_command_on(DEFAULT_CONNECTION_ID, make_hset_string_command(amx, params, 1, 2, 3)) ? 0 : -1;
}

cell redis_async_hset_integer(AMX* amx, cell* params)
{
    return enqueue_async_command_on(DEFAULT_CONNECTION_ID, make_hset_integer_command(amx, params, 1, 2, 3)) ? 0 : -1;
}

cell redis_async_set_string(AMX* amx, cell* params)
{
    return enqueue_async_command_on(DEFAULT_CONNECTION_ID, make_set_string_command(amx, params, 1, 2, 3, 4, 5)) ? 0 : -1;
}

cell redis_async_set_integer(AMX* amx, cell* params)
{
    return enqueue_async_command_on(DEFAULT_CONNECTION_ID, make_set_integer_command(amx, params, 1, 2, 3, 4, 5)) ? 0 : -1;
}

cell redis_async_del_key(AMX* amx, cell* params)
{
    return enqueue_async_command_on(DEFAULT_CONNECTION_ID, make_del_command(amx, params, 1)) ? 0 : -1;
}

cell redis_async_hdel_field(AMX* amx, cell* params)
{
    return enqueue_async_command_on(DEFAULT_CONNECTION_ID, make_hdel_command(amx, params, 1, 2)) ? 0 : -1;
}

cell redis_async_get_string(AMX* amx, cell* params)
{
    return enqueue_async_command_on(DEFAULT_CONNECTION_ID, make_get_command(amx, params, 1, 2)) ? 0 : -1;
}

cell redis_async_get_integer(AMX* amx, cell* params)
{
    return redis_async_get_string(amx, params);
}

cell redis_async_hget_string(AMX* amx, cell* params)
{
    return enqueue_async_command_on(DEFAULT_CONNECTION_ID, make_hget_command(amx, params, 1, 2, 3)) ? 0 : -1;
}

cell redis_async_hget_integer(AMX* amx, cell* params)
{
    return redis_async_hget_string(amx, params);
}

cell redis_async_queue_size(AMX* amx, cell* params)
{
    return redis_async_queue_size_on(amx, params);
}

cell redis_async_set_queue_limit(AMX* amx, cell* params)
{
    if (params[1] < 1)
    {
        return -1;
    }

    std::lock_guard<std::mutex> lock(g_connections_mutex);
    g_async_queue_limit = static_cast<size_t>(params[1]);
    for (auto& item : g_connections)
    {
        std::lock_guard<std::mutex> connection_lock(item.second->mutex);
        item.second->queue_limit = g_async_queue_limit;
    }
    return 0;
}

cell redis_async_last_error(AMX* amx, cell* params)
{
    std::string error;

    {
        std::lock_guard<std::mutex> lock(g_async_results_mutex);
        error = g_async_last_error;
    }

    MF_SetAmxString(amx, params[1], error.c_str(), params[2]);
    return error.empty() ? -1 : 0;
}

cell redis_async_last_error_on(AMX* amx, cell* params)
{
    auto connection = find_connection(params[1]);
    if (!connection)
    {
        const char* error = "async connection handle is invalid";
        MF_SetAmxString(amx, params[2], error, params[3]);
        set_global_async_error(error);
        return 0;
    }

    std::string error;
    {
        std::lock_guard<std::mutex> lock(connection->mutex);
        error = connection->last_error;
    }

    MF_SetAmxString(amx, params[2], error.c_str(), params[3]);
    return error.empty() ? -1 : 0;
}

cell redis_async_publish_on(AMX* amx, cell* params)
{
    int connection_id = get_param_count(params) >= 3 ? params[1] : DEFAULT_CONNECTION_ID;
    int channel_param = get_param_count(params) >= 3 ? 2 : 1;
    int message_param = get_param_count(params) >= 3 ? 3 : 2;
    return enqueue_async_command_on(connection_id, make_publish_command(amx, params, channel_param, message_param)) ? 0 : -1;
}

cell redis_async_hset_string_on(AMX* amx, cell* params)
{
    return enqueue_async_command_on(params[1], make_hset_string_command(amx, params, 2, 3, 4)) ? 0 : -1;
}

cell redis_async_hset_integer_on(AMX* amx, cell* params)
{
    return enqueue_async_command_on(params[1], make_hset_integer_command(amx, params, 2, 3, 4)) ? 0 : -1;
}

cell redis_async_set_string_on(AMX* amx, cell* params)
{
    return enqueue_async_command_on(params[1], make_set_string_command(amx, params, 2, 3, 4, 5, 6)) ? 0 : -1;
}

cell redis_async_set_integer_on(AMX* amx, cell* params)
{
    return enqueue_async_command_on(params[1], make_set_integer_command(amx, params, 2, 3, 4, 5, 6)) ? 0 : -1;
}

cell redis_async_del_key_on(AMX* amx, cell* params)
{
    return enqueue_async_command_on(params[1], make_del_command(amx, params, 2)) ? 0 : -1;
}

cell redis_async_hdel_field_on(AMX* amx, cell* params)
{
    return enqueue_async_command_on(params[1], make_hdel_command(amx, params, 2, 3)) ? 0 : -1;
}

cell redis_async_get_string_on(AMX* amx, cell* params)
{
    return enqueue_async_command_on(params[1], make_get_command(amx, params, 2, 3)) ? 0 : -1;
}

cell redis_async_get_integer_on(AMX* amx, cell* params)
{
    return redis_async_get_string_on(amx, params);
}

cell redis_async_hget_string_on(AMX* amx, cell* params)
{
    return enqueue_async_command_on(params[1], make_hget_command(amx, params, 2, 3, 4)) ? 0 : -1;
}

cell redis_async_hget_integer_on(AMX* amx, cell* params)
{
    return redis_async_hget_string_on(amx, params);
}

cell redis_async_queue_size_on(AMX* amx, cell* params)
{
    int connection_id = get_param_count(params) >= 1 ? params[1] : DEFAULT_CONNECTION_ID;
    auto connection = find_connection(connection_id);
    if (!connection)
    {
        return -1;
    }

    std::lock_guard<std::mutex> lock(connection->mutex);
    return static_cast<cell>(connection->queue.size());
}
