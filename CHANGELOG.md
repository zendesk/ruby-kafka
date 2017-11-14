# Changelog

Changes and additions to the library will be listed here.

## Unreleased

## v0.5.1

Requires Kafka 0.10.1+ due to usage of a few new APIs.

- Fix bug when using compression (#458).
- Update the v3 of the Fetch API, allowing a per-request `max_bytes` setting (#468).
- Make `#deliver_message` more resilient using retries and backoff.
- Add support for SASL SCRAM authentication (#465).
- Refactor and simplify SASL code.
- Fix issue when a consumer resets a partition to its default offset.
- Allow specifying a create time for messages (#481).

## v0.5.0

- Drops support for Kafka 0.9 in favor of Kafka 0.10 (#381)!
- Handle cases where there are no partitions to fetch from by sleeping a bit (#439).
- Handle problems with the broker cache (#440).
- Shut down more quickly (#438).

## v0.4.3

- Restart the async producer thread automatically after errors.
- Include the offset lag in batch consumer metrics (Statsd).
- Make the default `max_wait_time` more sane.
- Fix issue with cached default offset lookups (#431).
- Upgrade to Datadog client version 3.

## v0.4.2

- Fix connection issue on SASL connections (#401).
- Add more instrumentation of consumer groups (#407).
- Improve error logging (#385)

## v0.4.1

- Allow seeking the consumer position (#386).
- Reopen idle connections after 5 minutes (#399).

## v0.4.0

- Support SASL authentication (#334 and #370)
- Allow loading SSL certificates from files (#371)
- Add Statsd metric reporting (#373)

## v0.3.17

- Re-commit previously committed offsets periodically with an interval of half
  the offset retention time, starting with the first commit (#318).
- Expose offset retention time in the Consumer API (#316).
- Don't get blocked when there's temporarily no leader for a topic (#336).

## v0.3.16

- Fix SSL socket timeout (#283).
- Update to the latest Datadog gem (#296).
- Automatically detect private key type (#297).
- Only fetch messages for subscribed topics (#309).

## v0.3.15

- Allow setting a timeout on a partition pause (#272).
- Allow pausing consumption of a partition (#268).

## v0.3.14

- Automatically recover from invalid consumer checkpoints.
- Minimize the number of times messages are reprocessed after a consumer group resync.
- Improve instrumentation of the async producer.

## v0.3.12

- Fix a bug in the consumer.

## v0.3.11

- Fix bug in the simple consumer loop.

## v0.3.10

- Handle brokers becoming unavailable while in a consumer loop (#228).
- Handle edge case when consuming from the end of a topic (#230).
- Ensure the library can be loaded without Bundler (#224).
- Add an API for fetching the last offset in a partition (#232).

## v0.3.9

- Improve the default durability setting. The producer setting `required_acks` now defaults to `:all` (#210).
- Handle rebalances in the producer (#196). *Mpampis Kostas*
- Add simplified producer and consumer APIs for simple use cases.
- Add out-of-the-box Datadog reporting.
- Improve producer performance.

## v0.3.8

- Keep separate connection pools for consumers and producers initialized from
  the same client.
- Handle connection errors automatically in the async producer.

## v0.3.7

- Default to port 9092 if no port is provided for a seed broker.

## v0.3.6

- Fix bug that caused partition information to not be reliably updated.

## v0.3.5

- Fix bug that caused the async producer to not work with Unicorn (#166).
- Fix bug that caused committed consumer offsets to be lost (#167).
- Instrument buffer overflows in the producer.

## v0.3.4

- Make the producer buffer more resilient in the face of isolated topic errors.

## v0.3.3

- Allow clearing a producer's buffer (Martin Nowak).
- Improved Consumer API.
- Instrument producer errors.

## v0.3.2

- Experimental batch consumer API.

## v0.3.1

- Simplify the heartbeat algorithm.
- Handle partial messages at the end of message sets received from the brokers.

## v0.3.0

- Add support for encryption and authentication with SSL (Tom Crayford).
- Allow configuring consumer offset commit policies.
- Instrument consumer message processing.
- Fixed an issue causing exceptions when no logger was specified.

## v0.2.0

- Add instrumentation of message compression.
- **New!** Consumer API – still alpha level. Expect many changes.
