# Events Consumer #

Consumes the events beacon calls from kafka queue and stores into cassandra.

## Setup ##

- Clone the repo


## Running ##

It uses `god` [http://godrb.com](http://godrb.com) for monitoring process.

### Start

```
lang=bash
god -c apihandler.god
```

### Get Status

```
lang=bash
god status apiconsumer
```

### Stop

```
lang=bash
god stop apiconsumer
```



## Troubleshooting ##

### Logs

- Available at `CURR_DIR/shared/logs/`.
- Also check the `logfile` config at `config/config.yml`.

### consumer/partition_consumer.rb error ###

If you get error like

```
.../kafka/consumer/partition_consumer.rb:133:in `ensure in manage_partition_consumer': undefined method `close' for nil:NilClass (NoMethodError)

	from .../kafka/consumer/partition_consumer.rb:133:in `manage_partition_consumer'

	from .../kafka/consumer/partition_consumer.rb:16:in `block in initialize'

```

Just restart your kafka server.

### In case of most kafka fuckups

- restart kafka
