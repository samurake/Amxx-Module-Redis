#ifndef REDIS_ASYNC_SAFETY_H
#define REDIS_ASYNC_SAFETY_H

#include <cstddef>

namespace redis_async_safety
{
    static const std::size_t default_queue_limit = 4096;
    static const std::size_t max_queue_limit = 16384;
    static const std::size_t default_queue_byte_limit = 2 * 1024 * 1024;
    static const std::size_t max_connections = 16;
    static const std::size_t max_pending_connect_requests = 1024;
    static const std::size_t max_result_count = 16384;
    static const std::size_t max_error_bytes = 1024;
    static const std::size_t max_xadd_stream_bytes = 191;
    static const std::size_t max_xadd_event_id_bytes = 191;
    static const std::size_t max_xadd_payload_bytes = 8191;
    static const std::size_t max_xadd_per_second = 256;
    static const std::size_t max_redis_stream_id_bytes = 41;

    enum class XAddValidation
    {
        Valid,
        EmptyStream,
        StreamTooLarge,
        EmptyEventId,
        EventIdTooLarge,
        EmptyPayload,
        PayloadTooLarge,
        NegativeRequestId
    };

    inline XAddValidation validate_xadd(
        std::size_t stream_bytes,
        std::size_t event_id_bytes,
        std::size_t payload_bytes,
        int request_id
    )
    {
        if (stream_bytes == 0)
        {
            return XAddValidation::EmptyStream;
        }
        if (stream_bytes > max_xadd_stream_bytes)
        {
            return XAddValidation::StreamTooLarge;
        }
        if (event_id_bytes == 0)
        {
            return XAddValidation::EmptyEventId;
        }
        if (event_id_bytes > max_xadd_event_id_bytes)
        {
            return XAddValidation::EventIdTooLarge;
        }
        if (payload_bytes == 0)
        {
            return XAddValidation::EmptyPayload;
        }
        if (payload_bytes > max_xadd_payload_bytes)
        {
            return XAddValidation::PayloadTooLarge;
        }
        if (request_id < 0)
        {
            return XAddValidation::NegativeRequestId;
        }

        return XAddValidation::Valid;
    }

    template <typename Element>
    inline bool bounded_string_length(
        const Element* value,
        std::size_t maximum,
        std::size_t& length
    )
    {
        length = 0;
        if (value == nullptr)
        {
            return false;
        }

        while (length <= maximum)
        {
            if (value[length] == 0)
            {
                return true;
            }
            length++;
        }

        return false;
    }

    inline bool queue_has_capacity(
        std::size_t queue_size,
        std::size_t queue_limit,
        std::size_t queued_bytes,
        std::size_t queue_byte_limit,
        std::size_t command_bytes
    )
    {
        return queue_size < queue_limit
            && command_bytes <= queue_byte_limit
            && queued_bytes <= queue_byte_limit - command_bytes;
    }

    inline bool xadd_rate_has_capacity(std::size_t queued_in_window)
    {
        return queued_in_window < max_xadd_per_second;
    }

    inline bool is_redis_stream_id(const char* value, std::size_t length)
    {
        if (value == nullptr
            || length < 3
            || length > max_redis_stream_id_bytes)
        {
            return false;
        }

        bool separator_seen = false;
        for (std::size_t index = 0; index < length; ++index)
        {
            if (value[index] == '-')
            {
                if (separator_seen || index == 0 || index + 1 == length)
                {
                    return false;
                }
                separator_seen = true;
                continue;
            }
            if (value[index] < '0' || value[index] > '9')
            {
                return false;
            }
        }

        return separator_seen;
    }
}

#endif
