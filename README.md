# API Consumer #

Consumes the events beacon calls from kafka queue and stores into cassandra.

[![Build Status](https://travis-ci.org/octoai/apiconsumer.svg?branch=master)](https://travis-ci.org/octoai/apiconsumer)

# Setup #

- Clone the repo
- `git submodule init`
- Perform `bundle install`

# Start

### Creating kafka topic before starting consumer

You need to create the topic in kafka before you can start listening to it. There are two ways to do it

- Run `fakestream` which will push messages into the topic thus creating the topic itself.
- Create a topic manually in kafka using default settings. Here is how to do it

```bash
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic events
```


## Daemonized

```bash
ruby consumer.rb start
```

You can start multiple instances by running the above command multiple times.

### Get Status

```bash
ruby consumer.rb status
```

### Stop

```bash
ruby consumer.rb stop
```


### Logs

- Available at `PROJ_DIR/shared/logs/`
- Also check the `logfile` config at `config/config.yml`

## Foreground

```bash
FOREGROUND=true ruby consumer.rb
```
