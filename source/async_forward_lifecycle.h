#ifndef REDIS_ASYNC_FORWARD_LIFECYCLE_H
#define REDIS_ASYNC_FORWARD_LIFECYCLE_H

namespace redis_async_lifecycle
{
    class ForwardRegistry
    {
    public:
        void on_plugins_loaded()
        {
            plugins_loaded_ = true;
        }

        void on_plugins_unloading()
        {
            plugins_loaded_ = false;
            async_registered_ = false;
            subscriber_registered_ = false;
        }

        bool should_register_async() const
        {
            return plugins_loaded_ && !async_registered_;
        }

        bool should_register_subscriber() const
        {
            return plugins_loaded_ && !subscriber_registered_;
        }

        void mark_async_registered()
        {
            async_registered_ = true;
        }

        void mark_subscriber_registered()
        {
            subscriber_registered_ = true;
        }

        bool plugins_loaded() const
        {
            return plugins_loaded_;
        }

    private:
        bool plugins_loaded_ = false;
        bool async_registered_ = false;
        bool subscriber_registered_ = false;
    };
}

#endif
