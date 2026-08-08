#include "module.h"
#include "async_safety.h"

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
    const size_t DEFAULT_ASYNC_QUEUE_LIMIT =
        redis_async_safety::default_queue_limit;
    const size_t MAX_ASYNC_QUEUE_LIMIT =
        redis_async_safety::max_queue_limit;
    const size_t DEFAULT_ASYNC_QUEUE_BYTE_LIMIT =
        redis_async_safety::default_queue_byte_limit;
    const size_t MAX_ASYNC_CONNECTIONS =
        redis_async_safety::max_connections;
    const size_t MAX_PENDING_CONNECT_REQUESTS =
        redis_async_safety::max_pending_connect_requests;
    const size_t MAX_ASYNC_RESULT_COUNT =
        redis_async_safety::max_result_count;
    const size_t MAX_ASYNC_ERROR_BYTES =
        redis_async_safety::max_error_bytes;

    enum class AsyncCommandType
    {
        Publish,
        XAdd,
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
        size_t queue_limit = DEFAULT_ASYNC_QUEUE_LIMIT;
        size_t pending_command_count = 0;
        size_t queued_bytes = 0;
        size_t queue_byte_limit = DEFAULT_ASYNC_QUEUE_BYTE_LIMIT;
        std::set<int> pending_xadd_request_ids;
        std::chrono::steady_clock::time_point xadd_rate_window_started;
        size_t xadd_queued_in_window = 0;
        std::string last_error;
    };

    std::map<int, std::shared_ptr<AsyncConnection>> g_connections;
    std::mutex g_connections_mutex;
    std::deque<AsyncResult> g_async_results;
    std::deque<AsyncConnectResult> g_async_connect_results;
    std::mutex g_async_results_mutex;
    size_t g_async_dropped_results = 0;
    size_t g_async_dropped_connect_results = 0;
    size_t g_async_queue_limit = DEFAULT_ASYNC_QUEUE_LIMIT;
    std::string g_async_last_error;
    int g_next_connection_id = DEFAULT_CONNECTION_ID + 1;

    int get_param_count(cell* params)
    {
        return static_cast<int>(params[0] / sizeof(cell));
    }

    cell bounded_cell_count(size_t value)
    {
        const size_t maximum =
            static_cast<size_t>((std::numeric_limits<cell>::max)());
        return static_cast<cell>(value > maximum ? maximum : value);
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

    size_t async_command_bytes(const AsyncCommand& command)
    {
        return sizeof(AsyncCommand)
            + command.key.size()
            + command.field.size()
            + command.value.size();
    }

    void release_async_command_resources(
        const std::shared_ptr<AsyncConnection>& connection,
        const AsyncCommand& command
    )
    {
        const size_t bytes = async_command_bytes(command);
        std::lock_guard<std::mutex> lock(connection->mutex);
        if (connection->pending_command_count > 0)
        {
            connection->pending_command_count--;
        }
        connection->queued_bytes = bytes <= connection->queued_bytes
            ? connection->queued_bytes - bytes
            : 0;
    }

    std::string bounded_async_error(const std::string& error)
    {
        return error.size() <= MAX_ASYNC_ERROR_BYTES
            ? error
            : error.substr(0, MAX_ASYNC_ERROR_BYTES);
    }

    bool enqueue_async_result(const AsyncResult& result)
    {
        std::lock_guard<std::mutex> lock(g_async_results_mutex);
        if (g_async_results.size() >= MAX_ASYNC_RESULT_COUNT)
        {
            g_async_dropped_results++;
            g_async_last_error = "async result queue full";
            redis_set_last_error(g_async_last_error.c_str());
            return false;
        }
        g_async_results.push_back(result);
        return true;
    }

    void enqueue_async_connect_result(int connection_id, int request_id, int status, const std::string& error)
    {
        AsyncConnectResult result;
        result.connection_id = connection_id;
        result.request_id = request_id;
        result.status = status;
        result.error = bounded_async_error(error);

        std::lock_guard<std::mutex> lock(g_async_results_mutex);
        if (g_async_connect_results.size() >= MAX_ASYNC_RESULT_COUNT)
        {
            g_async_dropped_connect_results++;
            g_async_last_error = "async connection result queue full";
            redis_set_last_error(g_async_last_error.c_str());
            return;
        }
        g_async_connect_results.push_back(result);
    }

    void discard_async_results_for_connection(int connection_id)
    {
        std::lock_guard<std::mutex> lock(g_async_results_mutex);
        for (auto iter = g_async_results.begin();
            iter != g_async_results.end();)
        {
            if (iter->connection_id == connection_id)
            {
                iter = g_async_results.erase(iter);
            }
            else
            {
                ++iter;
            }
        }
        for (auto iter = g_async_connect_results.begin();
            iter != g_async_connect_results.end();)
        {
            if (iter->connection_id == connection_id)
            {
                iter = g_async_connect_results.erase(iter);
            }
            else
            {
                ++iter;
            }
        }
    }

    void release_pending_xadd_request(int connection_id, int request_id)
    {
        if (request_id <= 0)
        {
            return;
        }

        std::shared_ptr<AsyncConnection> connection;
        {
            std::lock_guard<std::mutex> lock(g_connections_mutex);
            auto iter = g_connections.find(connection_id);
            if (iter == g_connections.end())
            {
                return;
            }
            connection = iter->second;
        }

        std::lock_guard<std::mutex> lock(connection->mutex);
        connection->pending_xadd_request_ids.erase(request_id);
    }

    void enqueue_async_error_result(int connection_id, const AsyncCommand& command, const std::string& message)
    {
        if (command.type != AsyncCommandType::Get
            && command.type != AsyncCommandType::HGet
            && command.type != AsyncCommandType::XAdd)
        {
            return;
        }

        AsyncResult result;
        result.connection_id = connection_id;
        result.request_id = command.request_id;
        result.command = command.type == AsyncCommandType::Get
            ? "get"
            : (command.type == AsyncCommandType::HGet ? "hget" : "xadd");
        result.status = -1;
        result.key = command.key;
        result.field = command.field;
        result.value = bounded_async_error(message);
        if (!enqueue_async_result(result)
            && command.type == AsyncCommandType::XAdd)
        {
            release_pending_xadd_request(connection_id, command.request_id);
        }
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

            case AsyncCommandType::XAdd:
            {
                AsyncResult result;
                result.connection_id = connection_id;
                result.request_id = command.request_id;
                result.command = "xadd";
                result.status = 0;
                result.key = command.key;
                result.field = command.field;
                result.value = redis.command<std::string>(
                    "XADD",
                    command.key,
                    "*",
                    "event_id",
                    command.field,
                    "payload",
                    command.value
                );
                if (!redis_async_safety::is_redis_stream_id(
                    result.value.c_str(),
                    result.value.size()
                ))
                {
                    throw std::runtime_error(
                        "xadd returned an invalid Redis stream id"
                    );
                }
                if (!enqueue_async_result(result))
                {
                    release_pending_xadd_request(
                        connection_id,
                        command.request_id
                    );
                }
                break;
            }

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
                        release_async_command_resources(connection, command);
                    }
                    catch (const ReplyError& e)
                    {
                        // A Redis reply error (for example WRONGTYPE) is
                        // deterministic for this command. Retrying it would
                        // permanently head-of-line block the ordered queue.
                        set_connection_error(connection, e.what());
                        enqueue_async_error_result(connection->id, command, e.what());
                        release_async_command_resources(connection, command);
                        continue;
                    }
                    catch (const IoError& e)
                    {
                        {
                            // The command may or may not have reached Redis.
                            // Put it back at the head before reconnecting.
                            // Event IDs make ambiguous XADD retries idempotent
                            // at the application consumer.
                            std::lock_guard<std::mutex> lock(connection->mutex);
                            connection->queue.push_front(command);
                        }
                        set_connection_error(connection, e.what());
                        enqueue_async_connect_result(connection->id, 0, -1, e.what());
                        reconnect_notice_pending = true;
                        set_connection_state(connection, STATUS_RECONNECTING);
                        break;
                    }
                    catch (const ClosedError& e)
                    {
                        {
                            std::lock_guard<std::mutex> lock(connection->mutex);
                            connection->queue.push_front(command);
                        }
                        set_connection_error(connection, e.what());
                        enqueue_async_connect_result(connection->id, 0, -1, e.what());
                        reconnect_notice_pending = true;
                        set_connection_state(connection, STATUS_RECONNECTING);
                        break;
                    }
                    catch (const ProtoError& e)
                    {
                        {
                            std::lock_guard<std::mutex> lock(connection->mutex);
                            connection->queue.push_front(command);
                        }
                        set_connection_error(connection, e.what());
                        enqueue_async_connect_result(connection->id, 0, -1, e.what());
                        reconnect_notice_pending = true;
                        set_connection_state(connection, STATUS_RECONNECTING);
                        break;
                    }
                    catch (const Error& e)
                    {
                        // Formatting, range and local resource errors are not
                        // repaired by reconnecting. Report them once and keep
                        // later commands moving.
                        set_connection_error(connection, e.what());
                        enqueue_async_error_result(connection->id, command, e.what());
                        release_async_command_resources(connection, command);
                        continue;
                    }
                    catch (const std::exception& e)
                    {
                        set_connection_error(connection, e.what());
                        enqueue_async_error_result(connection->id, command, e.what());
                        release_async_command_resources(connection, command);
                        continue;
                    }
                    catch (...)
                    {
                        set_connection_error(connection, "unknown Redis async command error");
                        enqueue_async_error_result(connection->id, command, "unknown Redis async command error");
                        release_async_command_resources(connection, command);
                        continue;
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
        if (g_connections.size() >= MAX_ASYNC_CONNECTIONS)
        {
            set_global_async_error("async connection limit reached");
            enqueue_async_connect_result(
                use_default ? DEFAULT_CONNECTION_ID : 0,
                request_id,
                -1,
                "async connection limit reached"
            );
            return -1;
        }

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
                        if (connection->pending_connect_requests.size()
                            >= MAX_PENDING_CONNECT_REQUESTS)
                        {
                            error = "async connect request limit reached";
                            connection->last_error = error;
                            set_global_async_error(error);
                            enqueue_async_connect_result(
                                DEFAULT_CONNECTION_ID,
                                request_id,
                                -1,
                                error
                            );
                            return -1;
                        }
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

            const size_t command_bytes = async_command_bytes(command);
            if (!redis_async_safety::queue_has_capacity(
                connection->pending_command_count,
                connection->queue_limit,
                connection->queued_bytes,
                connection->queue_byte_limit,
                command_bytes
            ))
            {
                const char* error = connection->pending_command_count
                    >= connection->queue_limit
                        ? "async queue full"
                        : "async queue byte limit reached";
                connection->last_error = error;
                set_global_async_error(error);
                return false;
            }

            if (command.type == AsyncCommandType::XAdd
                && command.request_id > 0
                && connection->pending_xadd_request_ids.find(command.request_id)
                    != connection->pending_xadd_request_ids.end())
            {
                connection->last_error = "async xadd request_id is already pending";
                set_global_async_error(
                    "async xadd request_id is already pending"
                );
                return false;
            }

            if (command.type == AsyncCommandType::XAdd)
            {
                const std::chrono::steady_clock::time_point now =
                    std::chrono::steady_clock::now();
                if (connection->xadd_rate_window_started.time_since_epoch()
                        .count() == 0
                    || now - connection->xadd_rate_window_started
                        >= std::chrono::seconds(1))
                {
                    connection->xadd_rate_window_started = now;
                    connection->xadd_queued_in_window = 0;
                }
                if (!redis_async_safety::xadd_rate_has_capacity(
                    connection->xadd_queued_in_window
                ))
                {
                    connection->last_error = "async xadd rate limit reached";
                    set_global_async_error(
                        "async xadd rate limit reached"
                    );
                    return false;
                }
            }

            bool pending_xadd_inserted = false;
            if (command.type == AsyncCommandType::XAdd
                && command.request_id > 0)
            {
                pending_xadd_inserted =
                    connection->pending_xadd_request_ids.insert(
                        command.request_id
                    ).second;
            }

            try
            {
                connection->queue.push_back(command);
            }
            catch (...)
            {
                if (pending_xadd_inserted)
                {
                    connection->pending_xadd_request_ids.erase(
                        command.request_id
                    );
                }
                throw;
            }

            connection->pending_command_count++;
            connection->queued_bytes += command_bytes;
            if (command.type == AsyncCommandType::XAdd)
            {
                connection->xadd_queued_in_window++;
            }
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

    bool make_xadd_command(
        AMX* amx,
        cell* params,
        int stream_param,
        int event_id_param,
        int payload_param,
        int request_param,
        AsyncCommand& command,
        std::string& error
    )
    {
        command.type = AsyncCommandType::XAdd;
        command.request_id = params[request_param];
        if (command.request_id < 0)
        {
            error = "xadd request_id must be zero or positive";
            return false;
        }

        cell* stream_address = MF_GetAmxAddr(amx, params[stream_param]);
        cell* event_id_address = MF_GetAmxAddr(amx, params[event_id_param]);
        cell* payload_address = MF_GetAmxAddr(amx, params[payload_param]);
        if (stream_address == nullptr
            || event_id_address == nullptr
            || payload_address == nullptr)
        {
            error = "xadd input address is invalid";
            return false;
        }

        size_t stream_length = 0;
        size_t event_id_length = 0;
        size_t payload_length = 0;
        if (!redis_async_safety::bounded_string_length(
            stream_address,
            redis_async_safety::max_xadd_stream_bytes,
            stream_length
        ))
        {
            error = "xadd stream exceeds 191 bytes";
            return false;
        }
        if (!redis_async_safety::bounded_string_length(
            event_id_address,
            redis_async_safety::max_xadd_event_id_bytes,
            event_id_length
        ))
        {
            error = "xadd event_id exceeds 191 bytes";
            return false;
        }
        if (!redis_async_safety::bounded_string_length(
            payload_address,
            redis_async_safety::max_xadd_payload_bytes,
            payload_length
        ))
        {
            error = "xadd payload exceeds 8191 bytes";
            return false;
        }

        const redis_async_safety::XAddValidation validation =
            redis_async_safety::validate_xadd(
                stream_length,
                event_id_length,
                payload_length,
                command.request_id
            );
        switch (validation)
        {
            case redis_async_safety::XAddValidation::Valid:
                break;
            case redis_async_safety::XAddValidation::EmptyStream:
                error = "xadd stream is empty";
                break;
            case redis_async_safety::XAddValidation::StreamTooLarge:
                error = "xadd stream exceeds 191 bytes";
                break;
            case redis_async_safety::XAddValidation::EmptyEventId:
                error = "xadd event_id is empty";
                break;
            case redis_async_safety::XAddValidation::EventIdTooLarge:
                error = "xadd event_id exceeds 191 bytes";
                break;
            case redis_async_safety::XAddValidation::EmptyPayload:
                error = "xadd payload is empty";
                break;
            case redis_async_safety::XAddValidation::PayloadTooLarge:
                error = "xadd payload exceeds 8191 bytes";
                break;
            case redis_async_safety::XAddValidation::NegativeRequestId:
                error = "xadd request_id must be zero or positive";
                break;
        }

        if (validation != redis_async_safety::XAddValidation::Valid)
        {
            return false;
        }

        int copied_length = 0;
        command.key = MF_GetAmxString(
            amx,
            params[stream_param],
            0,
            &copied_length
        );
        command.field = MF_GetAmxString(
            amx,
            params[event_id_param],
            1,
            &copied_length
        );
        command.value = MF_GetAmxString(
            amx,
            params[payload_param],
            2,
            &copied_length
        );

        if (command.key.size() != stream_length
            || command.field.size() != event_id_length
            || command.value.size() != payload_length)
        {
            error = "xadd input changed while being copied";
            return false;
        }

        return true;
    }

    cell enqueue_xadd_command_on(
        AMX* amx,
        cell* params,
        int connection_id,
        int stream_param,
        int event_id_param,
        int payload_param,
        int request_param
    )
    {
        try
        {
            AsyncCommand command;
            std::string error;
            if (!make_xadd_command(
                amx,
                params,
                stream_param,
                event_id_param,
                payload_param,
                request_param,
                command,
                error
            ))
            {
                set_global_async_error(error);
                std::shared_ptr<AsyncConnection> connection =
                    find_connection(connection_id);
                if (connection)
                {
                    set_connection_error(connection, error);
                }
                return -1;
            }

            return enqueue_async_command_on(
                connection_id,
                command
            ) ? 0 : -1;
        }
        catch (const std::exception& exception)
        {
            const std::string error = bounded_async_error(exception.what());
            set_global_async_error(error);
            std::shared_ptr<AsyncConnection> connection =
                find_connection(connection_id);
            if (connection)
            {
                set_connection_error(connection, error);
            }
            return -1;
        }
        catch (...)
        {
            const std::string error = "unknown xadd enqueue error";
            set_global_async_error(error);
            std::shared_ptr<AsyncConnection> connection =
                find_connection(connection_id);
            if (connection)
            {
                set_connection_error(connection, error);
            }
            return -1;
        }
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

        if (result.command == "xadd")
        {
            // Release immediately before invoking Pawn so a callback can
            // safely retry the same logical slot without racing a stale
            // request that is still queued or awaiting dispatch.
            release_pending_xadd_request(
                result.connection_id,
                result.request_id
            );
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
    try
    {
        ConnectionOptions options = read_options(amx, params, 1, 2, 3, 4);
        return open_async_connection(options, params[5], "", true, true);
    }
    catch (const std::exception& exception)
    {
        set_global_async_error(bounded_async_error(exception.what()));
        return -1;
    }
    catch (...)
    {
        set_global_async_error("unknown async connect error");
        return -1;
    }
}

cell redis_async_open(AMX* amx, cell* params)
{
    try
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
    catch (const std::exception& exception)
    {
        set_global_async_error(bounded_async_error(exception.what()));
        return -1;
    }
    catch (...)
    {
        set_global_async_error("unknown async open error");
        return -1;
    }
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
    discard_async_results_for_connection(connection_id);
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

cell redis_async_xadd(AMX* amx, cell* params)
{
    return enqueue_xadd_command_on(
        amx,
        params,
        DEFAULT_CONNECTION_ID,
        1,
        2,
        3,
        4
    );
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

cell redis_async_queue_bytes(AMX* amx, cell* params)
{
    return redis_async_queue_bytes_on(amx, params);
}

cell redis_async_dropped_results(AMX* amx, cell* params)
{
    std::lock_guard<std::mutex> lock(g_async_results_mutex);
    const size_t maximum =
        static_cast<size_t>((std::numeric_limits<cell>::max)());
    if (g_async_dropped_results >= maximum
        || g_async_dropped_connect_results
            >= maximum - g_async_dropped_results)
    {
        return (std::numeric_limits<cell>::max)();
    }
    return static_cast<cell>(
        g_async_dropped_results + g_async_dropped_connect_results
    );
}

cell redis_async_set_queue_limit(AMX* amx, cell* params)
{
    if (params[1] < 1
        || static_cast<size_t>(params[1]) > MAX_ASYNC_QUEUE_LIMIT)
    {
        set_global_async_error(
            "async queue limit must be between 1 and 16384"
        );
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

cell redis_async_xadd_on(AMX* amx, cell* params)
{
    return enqueue_xadd_command_on(
        amx,
        params,
        params[1],
        2,
        3,
        4,
        5
    );
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
    return static_cast<cell>(connection->pending_command_count);
}

cell redis_async_queue_bytes_on(AMX* amx, cell* params)
{
    int connection_id = get_param_count(params) >= 1
        ? params[1]
        : DEFAULT_CONNECTION_ID;
    auto connection = find_connection(connection_id);
    if (!connection)
    {
        return -1;
    }

    std::lock_guard<std::mutex> lock(connection->mutex);
    return bounded_cell_count(connection->queued_bytes);
}
