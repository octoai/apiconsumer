# Events Consumer #

Consumes the events beacon calls from kafka queue and stores into cassandra.

## Setup ##

- Clone the repo


## Running ##

- Run `ruby consumer.rb > /tmp/consumer.log`
- View the logfile at `/tmp/consumer.log`
- Close by `CTRL + c`

Would be daemonised

## Troubleshooting ##

### consumer/partition_consumer.rb error ###

If you get error like

```
.../kafka/consumer/partition_consumer.rb:133:in `ensure in manage_partition_consumer': undefined method `close' for nil:NilClass (NoMethodError)

	from .../kafka/consumer/partition_consumer.rb:133:in `manage_partition_consumer'

	from .../kafka/consumer/partition_consumer.rb:16:in `block in initialize'

```

Just restart your kafka server.
