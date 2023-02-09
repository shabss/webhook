
MESSAGES_DB = "messages"
# MessagesDB:
#   key: (source_id, message_id)
#   value: (payload (json), status)

MESSAGE_TRACKING_DB = "message_tracking"
# Messages Tracking DB:
#   key: (source_id, message_id)
#   value: (retries, last_sent)

RECEIVER_CONFIG_DB = "receiver_config"
# Receiver Config DB:
#   key: (receiver name)
#   value:
#       rate (msg/s),
#       window_size (sec),
#       window (msgs/window_size),
#       endpoint,
#       method,
#       async_endpoint

