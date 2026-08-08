#include "../source/async_safety.h"
#include "../source/async_forward_lifecycle.h"

#include <cassert>
#include <cstddef>
#include <limits>

using redis_async_safety::XAddValidation;

int main()
{
    {
        redis_async_lifecycle::ForwardRegistry registry;
        assert(!registry.plugins_loaded());
        assert(!registry.should_register_async());
        assert(!registry.should_register_subscriber());

        registry.on_plugins_loaded();
        assert(registry.plugins_loaded());
        assert(registry.should_register_async());
        assert(registry.should_register_subscriber());

        registry.mark_async_registered();
        registry.mark_subscriber_registered();
        assert(!registry.should_register_async());
        assert(!registry.should_register_subscriber());

        registry.on_plugins_unloading();
        assert(!registry.plugins_loaded());
        assert(!registry.should_register_async());
        assert(!registry.should_register_subscriber());

        registry.on_plugins_loaded();
        assert(registry.should_register_async());
        assert(registry.should_register_subscriber());
    }

    using namespace redis_async_safety;

    assert(validate_xadd(1, 1, 1, 0) == XAddValidation::Valid);
    assert(validate_xadd(
        max_xadd_stream_bytes,
        max_xadd_event_id_bytes,
        max_xadd_payload_bytes,
        1
    ) == XAddValidation::Valid);
    assert(validate_xadd(0, 1, 1, 1) == XAddValidation::EmptyStream);
    assert(validate_xadd(
        max_xadd_stream_bytes + 1,
        1,
        1,
        1
    ) == XAddValidation::StreamTooLarge);
    assert(validate_xadd(1, 0, 1, 1) == XAddValidation::EmptyEventId);
    assert(validate_xadd(
        1,
        max_xadd_event_id_bytes + 1,
        1,
        1
    ) == XAddValidation::EventIdTooLarge);
    assert(validate_xadd(1, 1, 0, 1) == XAddValidation::EmptyPayload);
    assert(validate_xadd(
        1,
        1,
        max_xadd_payload_bytes + 1,
        1
    ) == XAddValidation::PayloadTooLarge);
    assert(validate_xadd(1, 1, 1, -1) == XAddValidation::NegativeRequestId);

    const char exact_length[] = {'a', 'b', 'c', '\0'};
    const char too_large[] = {'a', 'b', 'c', 'd', '\0'};
    std::size_t measured_length = 99;
    assert(bounded_string_length(
        exact_length,
        3,
        measured_length
    ));
    assert(measured_length == 3);
    assert(!bounded_string_length(
        too_large,
        3,
        measured_length
    ));
    assert(measured_length == 4);
    assert(!bounded_string_length<char>(nullptr, 3, measured_length));
    assert(measured_length == 0);

    assert(queue_has_capacity(0, 1, 0, 100, 100));
    assert(!queue_has_capacity(1, 1, 0, 100, 1));
    assert(!queue_has_capacity(0, 1, 100, 100, 1));
    assert(!queue_has_capacity(0, 1, 0, 100, 101));
    assert(!queue_has_capacity(
        0,
        1,
        std::numeric_limits<std::size_t>::max(),
        100,
        1
    ));
    assert(max_pending_connect_requests < max_result_count);
    assert(xadd_rate_has_capacity(0));
    assert(xadd_rate_has_capacity(max_xadd_per_second - 1));
    assert(!xadd_rate_has_capacity(max_xadd_per_second));
    assert(is_redis_stream_id("1-0", 3));
    assert(is_redis_stream_id("18446744073709551615-42", 23));
    assert(!is_redis_stream_id(nullptr, 0));
    assert(!is_redis_stream_id("10", 2));
    assert(!is_redis_stream_id("-1", 2));
    assert(!is_redis_stream_id("1-", 2));
    assert(!is_redis_stream_id("1-2-3", 5));
    assert(!is_redis_stream_id("1-a", 3));
    assert(!is_redis_stream_id(
        "123456789012345678901-12345678901234567890",
        42
    ));

    return 0;
}
