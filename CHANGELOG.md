# Changelog

Changes and additions to the library will be listed here.

## Unreleased

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
