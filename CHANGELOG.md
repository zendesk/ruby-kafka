# Changelog

Changes and additions to the library will be listed here.

## Unreleased

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
