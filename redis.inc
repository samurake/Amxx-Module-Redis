#if defined _redis_included
	#endinput
#endif
#define _redis_included


/*
 * @note	Creates a Redis connection with the specified parameters.
 * 
 * @param	hostip[]	Redis server IP address. (IPv4)
 * @param	port		Redis server port number.(Default: 6379)
 * @param	username[]	Username specified for the Redis server. (Default: blank)
 * @param	password[]	Password specified for the Redis server. (Default: blank)
 * 
 * @note	Execute within the `plugin_init` function.
 * 
 * @return	0 is success. -1 is error.
 */
native redis_connect(const hostip[], const port = 6379, const username[] = "", const password[] = "");

/*
 * @note	When using the Pub/Sub command, register a subscriber channel.
 * 
 * @param	channel[]	Specified channel name.
 * 
 * @note	Execute within the `plugin_init` function. 
 *			Also, execute after the `redis_connect` function.
 * 
 * @return	0 is success. -1 is error.
 */
native redis_register_subscriber(const channel[]);

/*
 * @note	Use the `SET` command to write values to the Redis server. (for string)
 * 
 * @param	key[]		key name.
 * @param	value[]		value of string.
 * @param   ttl         Optional time-to-live in seconds. Default = 0 (no TTL).
 * @param   type        Optional update type:
 *                      0 = ALWAYS: overwrite the key regardless of existence (default)
 *                      1 = NX    : set only if key does NOT exist
 *                      2 = XX    : set only if key already exists
 * @param   keepttl     Optional flag. If non-zero, keeps the existing TTL on the key
 *                      instead of overwriting it. Default = 0 (ignore)
 * 
 * @return	0 is success. -1 is error.
 */
native redis_set_string(const key[], const value[], const ttl = 0, const type = 0, const keepttl = 0);

/*
 * @note	Use the `SET` command to write values to the Redis server. (for integer)
 * 
 * @param	key[]		key name.
 * @param	value		value of integer.
 * @param   ttl         Optional time-to-live in seconds. Default = 0 (no TTL).
 * @param   type        Optional update type:
 *                      0 = ALWAYS: overwrite the key regardless of existence (default)
 *                      1 = NX    : set only if key does NOT exist
 *                      2 = XX    : set only if key already exists
 * @param   keepttl     Optional flag. If non-zero, keeps the existing TTL on the key
 *                      instead of overwriting it. Default = 0 (ignore)
 * 
 * @return	0 is success. -1 is error.
 */
native redis_set_integer(const key[], const value, const ttl = 0, const type = 0, const keepttl = 0);

/*
 * @note	Use the `GET` command to read values into the Redis server. (for string)
 *
 * @param	key[]		key name.
 * 
 * @ref		value[]		get value of string.
 * @ref		maxlength[]	get length of value.
 *
 * @return	0 is success. -1 is error.
 */
native redis_get_string(const key[], value[], maxlength);

/*
 * @note	Use the `GET` command to read values into the Redis server. (for integer)
 *
 * @param	key[]		key name.
 *
 * @return	get value. (error is 0)
 */
native redis_get_integer(const key[]);

/*
 * @note	Use the `HSET` command to write values to the Redis server. (for string)
 *
 * @param	key[]		key name.
 * @param	field[]		field name.
 * @param	value[]		value of string.
 *
 * @return	0 is success. -1 is error.
 */
native redis_hset_string(const key[], const field[], const value[]);

/*
 * @note	Use the `HSET` command to write values to the Redis server. (for integer)
 *
 * @param	key[]		key name.
 * @param	field[]		field name.
 * @param	value		value of integer.
 *
 * @return	0 is success. -1 is error.
 */
native redis_hset_integer(const key[], const field[], const value);

/*
 * @note	Use the `HGET` command to read values into the Redis server. (for string)
 *
 * @param	key[]		key name.
 * @param	field[]		field name.
 * 
 * @ref		value[]		get value of string.
 * @ref		maxlength	get length of value
 * @return	0 is success. -1 is error.
 */
native redis_hget_string(const key[], const field[], value[], maxlength);

/*
 * @note	Use the `HGET` command to read values into the Redis server. (for integer)
 *
 * @param	key[]		key name.
 * @param	field[]		field name.
 *
 * @return	get value. (error is 0)
 */
native redis_hget_integer(const key[], const field[]);

/*
 * @note	Use the `DEL` command to delete values into the Redis server.
 *
 * @param	key[]		key name.
 *
 * @return	0 is success. -1 is error.
 */
native redis_del_key(const key[]);

/*
 * @note	Use the `HDEL` command to delete values into the Redis server.
 *
 * @param	key[]		key name.
 * @param	field[]		field name.
 *
 * @return	0 is success. -1 is error.
 */
native redis_hdel_field(const key[], const field[]);

/*
 * @note	Use the `PUB` command to send messages to the Redis server.
 *
 * @param	channel[]	channel name.
 * @param	message[]	value of message.
 *
 * @return	0 is success. -1 is error.
 */
native redis_publish(const channel[], const message[]);

/*
 * @note    Enqueue a PUBLISH command on the Redis worker thread.
 *          This native returns after queueing and does not wait for Redis I/O.
 *
 * @param   channel[]   channel name.
 * @param   message[]   value of message.
 *
 * @return  0 is queued. -1 is error or queue full.
 */
native redis_async_publish(const channel[], const message[]);

/*
 * @note    Enqueue a HSET command on the Redis worker thread. (for string)
 *
 * @return  0 is queued. -1 is error or queue full.
 */
native redis_async_hset_string(const key[], const field[], const value[]);

/*
 * @note    Enqueue a HSET command on the Redis worker thread. (for integer)
 *
 * @return  0 is queued. -1 is error or queue full.
 */
native redis_async_hset_integer(const key[], const field[], const value);

/*
 * @note    Enqueue a SET command on the Redis worker thread. (for string)
 *
 * @return  0 is queued. -1 is error or queue full.
 */
native redis_async_set_string(const key[], const value[], const ttl = 0, const type = 0, const keepttl = 0);

/*
 * @note    Enqueue a SET command on the Redis worker thread. (for integer)
 *
 * @return  0 is queued. -1 is error or queue full.
 */
native redis_async_set_integer(const key[], const value, const ttl = 0, const type = 0, const keepttl = 0);

/*
 * @note    Enqueue a DEL command on the Redis worker thread.
 *
 * @return  0 is queued. -1 is error or queue full.
 */
native redis_async_del_key(const key[]);

/*
 * @note    Enqueue a HDEL command on the Redis worker thread.
 *
 * @return  0 is queued. -1 is error or queue full.
 */
native redis_async_hdel_field(const key[], const field[]);

/*
 * @note    Returns the current number of queued async commands.
 */
native redis_async_queue_size();

/*
 * @note    Sets the maximum async queue depth. Default is 4096.
 *
 * @return  0 is success. -1 is invalid limit.
 */
native redis_async_set_queue_limit(limit);

/*
 * @note    Copies the last async worker error into output.
 *
 * @return  0 if an error exists. -1 if no error has been recorded.
 */
native redis_async_last_error(output[], maxlength);

/*
 * @note	When a message is received via the PUB/SUB command, this function is called as a callback.
 * 
 * @param	channel[]	Channel name of the received message.
 * @param	message[]	Content of the received message.
 * 
 * example:
 * public Redis_Subscriber_OnMessage(channel[], message[])
 * {
 *     // Check exists channel.
 *     if (equali(channel, "CHANNEL:NAME"))
 *     {
 *         client_print(0, print_chat, message); // Displayed in all player chat.
 *     }
 * }
 *
 */
forward Redis_Subscriber_OnMessage(channel[], message[]);
