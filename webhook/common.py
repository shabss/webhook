
MESSAGES_DB = "messages"
# MessagesDB:
#   key: (source_id, message_id)
#   value: (payload (json), status)

MESSAGE_TRACKING_DB = "message_tracking"
# Messages Tracking DB:
#   key: (source_id, message_id)
#   value: (retries, last_sent)

MESSAGE_TTL_DB = "message_ttl"
# Messages TTL DB:
#   key: (source_id, message_id)

RECEIVER_CONFIG_DB = "receiver_config"
# Receiver Config DB:
#   key: (receiver name)
#   value:
#       rate (msg/sec),
#       window_size (sec),
#       window (msgs/window_size),
#       retry_after (sec),
#       endpoint,
#       method,
#       async_endpoint

class HTTPClient:
    pass
