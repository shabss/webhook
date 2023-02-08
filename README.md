# Webhook Design


## Architecture

We use **Kafka** as a queueing system because it offers following features:
1. High Availability
2. Message ordering (per partition)
3. Manual commit. Helps retrying failed operations

We use **Redis** as our primary database becasue it offers the following features:
1. Highly available
2. In memory fast access
3. Can be configured to be backed by disks

We use **Sentry** for monitoring and exception traking

### Forward Flow

    Source -> Kafka -> Worker1 -> Kafka -> Worker2 -> Receiver 
                         |                    |
                       (dedup)            (rate limit)

### Backward Flow (synchronous)

    Source <- Worker3 -> Kafka <- Worker2 <- Receiver 

### Backward Flow (asynchronous)

                                  Worker4 -> Receiver 
    Source <- Worker3 -> Kafka <- Worker4 <- Receiver 
