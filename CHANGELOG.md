# Changelog

Changes and additions to the library will be listed here.

## Unreleased

## 0.7.8
- Optionally verify hostname on SSL certs ([#733](https://github.com/zendesk/ruby-kafka/pull/33))

## 0.7.7
- Producer send offsets in transaction ([#723](https://github.com/zendesk/ruby-kafka/pull/723))
- Support zstd compression ([#724](https://github.com/zendesk/ruby-kafka/pull/724))
- Verify SSL Certificates ([#730](https://github.com/zendesk/ruby-kafka/pull/730))

## 0.7.6
- Introduce regex matching in `Consumer#subscribe` ([#700](https://github.com/zendesk/ruby-kafka/pull/700))
- Only rejoin group on error if we're not in shutdown mode ([#711](https://github.com/zendesk/ruby-kafka/pull/711))
- Use `maxTimestamp` for `logAppendTime` timestamps ([#706](https://github.com/zendesk/ruby-kafka/pull/706))
- Async producer limit number of retries ([#708](https://github.com/zendesk/ruby-kafka/pull/708))
- Support SASL OAuthBearer Authentication ([#710](https://github.com/zendesk/ruby-kafka/pull/710))

## 0.7.5
- Distribute partitions across consumer groups when there are few partitions per topic ([#681](https://github.com/zendesk/ruby-kafka/pull/681))
- Fix an issue where a consumer would fail to fetch any messages ([#689](https://github.com/zendesk/ruby-kafka/pull/689))
- Instrumentation for heartbeat event
- Synchronously stop the fetcher to prevent race condition when processing commands
- Instrument batch fetching ([#694](https://github.com/zendesk/ruby-kafka/pull/694))

## 0.7.4
- Fix wrong encoding calculation that leads to message corruption ([#682](https://github.com/zendesk/ruby-kafka/pull/682), [#680](https://github.com/zendesk/ruby-kafka/pull/680)).
- Change the log level of the 'Committing offsets' message to debug ([#640](https://github.com/zendesk/ruby-kafka/pull/640)).
- Avoid Ruby warnings about unused vars ([#679](https://github.com/zendesk/ruby-kafka/pull/679)).
- Synchronously commit offsets after HeartbeatError ([#676](https://github.com/zendesk/ruby-kafka/pull/676)).
- Discard messages that were fetched under a previous consumer group generation ([#665](https://github.com/zendesk/ruby-kafka/pull/665)).
- Support specifying an ssl client certificates key passphrase ([#667](https://github.com/zendesk/ruby-kafka/pull/667)).

## 0.7.3

- Synchronize access to @worker_thread and @timer_thread in AsyncProducer to prevent creating multiple threads ([#661](https://github.com/zendesk/ruby-kafka/pull/661)).

## 0.7.2

- Handle case when paused partition does not belong to group on resume ([#656](https://github.com/zendesk/ruby-kafka/pull/656)).
- Fix compatibility version in documentation ([#651](https://github.com/zendesk/ruby-kafka/pull/651)).
- Fix message set backward compatible ([#648](https://github.com/zendesk/ruby-kafka/pull/648)).
- Refresh metadata on connection error when listing topics ([#644](https://github.com/zendesk/ruby-kafka/pull/644)).

## 0.7.1

- Compatibility with dogstatsd-ruby v4.0.0.
- Fix consuming duplication due to redundant messages returned from Kafka ([#636](https://github.com/zendesk/ruby-kafka/pull/636)).
- Fresh cluster info on fetch error ([#641](https://github.com/zendesk/ruby-kafka/pull/641)).
- Exactly Once Delivery and Transactional Messaging Support ([#608](https://github.com/zendesk/ruby-kafka/pull/608)).
- Support extra client certificates in the SSL Context when authenticating with Kafka ([#633](https://github.com/zendesk/ruby-kafka/pull/633)).

## 0.7.0

- Drop support for Kafka 0.10 in favor of native support for Kafka 0.11.
- Support record headers ([#604](https://github.com/zendesk/ruby-kafka/pull/604)).
- Add instrumenter and logger when async message delivery fails ([#603](https://github.com/zendesk/ruby-kafka/pull/603)).
- Upgrade and rename GroupCoordinator API to FindCoordinator API ([#606](https://github.com/zendesk/ruby-kafka/pull/606)).
- Refresh cluster metadata after topic re-assignment ([#609](https://github.com/zendesk/ruby-kafka/pull/609)).
- Disable SASL over SSL with a new config ([#613](https://github.com/zendesk/ruby-kafka/pull/613)).
- Allow listing brokers in a cluster ([#626](https://github.com/zendesk/ruby-kafka/pull/626)).
- Fix Fetcher's message skipping ([#625](https://github.com/zendesk/ruby-kafka/pull/625)).

## 0.6.7

- Handle case where consumer doesn't know about the topic ([#597](https://github.com/zendesk/ruby-kafka/pull/597) + [0e302cbd0f31315bf81c1d1645520413ad6b58f0](https://github.com/zendesk/ruby-kafka/commit/0e302cbd0f31315bf81c1d1645520413ad6b58f0))

## v0.6.5

- Fix bug related to partition assignment.

## v0.6.4

- Fix bug that caused consumers to jump back and reprocess messages ([#595](https://github.com/zendesk/ruby-kafka/pull/595)).

## v0.6.3

- Allow configuring the max size of the queue connecting the fetcher thread with the consumer.
- Add support for the Describe Groups API ([#583](https://github.com/zendesk/ruby-kafka/pull/583)).

## v0.6.2

- Add list groups API ([#582](https://github.com/zendesk/ruby-kafka/pull/582)).
- Use mutable String constructor ([#584](https://github.com/zendesk/ruby-kafka/pull/584)).

## v0.6.1

- Fix bug with exponential pausing causing pauses never to stop.

## v0.6.0

- Fetch messages asynchronously ([#526](https://github.com/zendesk/ruby-kafka/pull/526)).
- Add support for exponential backoff in pauses ([#566](https://github.com/zendesk/ruby-kafka/pull/566)).
- Instrument pause durations ([#574](https://github.com/zendesk/ruby-kafka/pull/574)).

## v0.5.5

- Support PLAINTEXT and SSL URI schemes ([#550](https://github.com/zendesk/ruby-kafka/pull/550)).

## v0.5.4

- Add support for config entries in the topic creation API ([#540](https://github.com/zendesk/ruby-kafka/pull/540)).
- Don't fail on retry when the cluster is secured ([#545](https://github.com/zendesk/ruby-kafka/pull/545)).

## v0.5.3

- Add support for the topic deletion API ([#528](https://github.com/zendesk/ruby-kafka/pull/528)).
- Add support for the partition creation API ([#533](https://github.com/zendesk/ruby-kafka/pull/533)).
- Allow passing in the seed brokers in a positional argument ([#538](https://github.com/zendesk/ruby-kafka/pull/538)).

## v0.5.2

- Instrument the start of message/batch processing ([#496](https://github.com/zendesk/ruby-kafka/pull/496)).
- Mark `Client#fetch_messages` as stable.
- Fix the list topics API ([#508](https://github.com/zendesk/ruby-kafka/pull/508)).
- Add support for LZ4 compression ([#499](https://github.com/zendesk/ruby-kafka/pull/499)).
- Refactor compression codec lookup ([#509](https://github.com/zendesk/ruby-kafka/pull/509)).
- Fix compressed message set offset bug ([#506](https://github.com/zendesk/ruby-kafka/pull/506)).
- Test against multiple versions of Kafka.
- Fix double-processing of messages after a consumer exception ([#518](https://github.com/zendesk/ruby-kafka/pull/518)).
- Track consumer offsets in Datadog.

## v0.5.1

Requires Kafka 0.10.1+ due to usage of a few new APIs.

- Fix bug when using compression ([#458](https://github.com/zendesk/ruby-kafka/pull/458)).
- Update the v3 of the Fetch API, allowing a per-request `max_bytes` setting ([#468](https://github.com/zendesk/ruby-kafka/pull/468)).
- Make `#deliver_message` more resilient using retries and backoff.
- Add support for SASL SCRAM authentication ([#465](https://github.com/zendesk/ruby-kafka/pull/465)).
- Refactor and simplify SASL code.
- Fix issue when a consumer resets a partition to its default offset.
- Allow specifying a create time for messages ([#481](https://github.com/zendesk/ruby-kafka/pull/481)).

## v0.5.0

- Drops support for Kafka 0.9 in favor of Kafka 0.10 ([#381](https://github.com/zendesk/ruby-kafka/pull/381))!
- Handle cases where there are no partitions to fetch from by sleeping a bit ([#439](https://github.com/zendesk/ruby-kafka/pull/439)).
- Handle problems with the broker cache ([#440](https://github.com/zendesk/ruby-kafka/pull/440)).
- Shut down more quickly ([#438](https://github.com/zendesk/ruby-kafka/pull/438)).

## v0.4.3

- Restart the async producer thread automatically after errors.
- Include the offset lag in batch consumer metrics (Statsd).
- Make the default `max_wait_time` more sane.
- Fix issue with cached default offset lookups ([#431](https://github.com/zendesk/ruby-kafka/pull/431)).
- Upgrade to Datadog client version 3.

## v0.4.2

- Fix connection issue on SASL connections ([#401](https://github.com/zendesk/ruby-kafka/pull/401)).
- Add more instrumentation of consumer groups ([#407](https://github.com/zendesk/ruby-kafka/pull/407)).
- Improve error logging ([#385](https://github.com/zendesk/ruby-kafka/pull/385))

## v0.4.1

- Allow seeking the consumer position ([#386](https://github.com/zendesk/ruby-kafka/pull/386)).
- Reopen idle connections after 5 minutes ([#399](https://github.com/zendesk/ruby-kafka/pull/399)).

## v0.4.0

- Support SASL authentication ([#334](https://github.com/zendesk/ruby-kafka/pull/334) and [#370](https://github.com/zendesk/ruby-kafka/pull/334))
- Allow loading SSL certificates from files ([#371](https://github.com/zendesk/ruby-kafka/pull/371))
- Add Statsd metric reporting ([#373](https://github.com/zendesk/ruby-kafka/pull/373))

## v0.3.17

- Re-commit previously committed offsets periodically with an interval of half
  the offset retention time, starting with the first commit ([#318](https://github.com/zendesk/ruby-kafka/pull/318)).
- Expose offset retention time in the Consumer API ([#316](https://github.com/zendesk/ruby-kafka/pull/316)).
- Don't get blocked when there's temporarily no leader for a topic ([#336](https://github.com/zendesk/ruby-kafka/pull/336)).

## v0.3.16

- Fix SSL socket timeout ([#283](https://github.com/zendesk/ruby-kafka/pull/283)).
- Update to the latest Datadog gem ([#296](https://github.com/zendesk/ruby-kafka/pull/296)).
- Automatically detect private key type ([#297](https://github.com/zendesk/ruby-kafka/pull/297)).
- Only fetch messages for subscribed topics ([#309](https://github.com/zendesk/ruby-kafka/pull/309)).

## v0.3.15

- Allow setting a timeout on a partition pause ([#272](https://github.com/zendesk/ruby-kafka/pull/272)).
- Allow pausing consumption of a partition ([#268](https://github.com/zendesk/ruby-kafka/pull/268)).

## v0.3.14

- Automatically recover from invalid consumer checkpoints.
- Minimize the number of times messages are reprocessed after a consumer group resync.
- Improve instrumentation of the async producer.

## v0.3.12

- Fix a bug in the consumer.

## v0.3.11

- Fix bug in the simple consumer loop.

## v0.3.10

- Handle brokers becoming unavailable while in a consumer loop ([#228](https://github.com/zendesk/ruby-kafka/pull/228)).
- Handle edge case when consuming from the end of a topic ([#230](https://github.com/zendesk/ruby-kafka/pull/230)).
- Ensure the library can be loaded without Bundler ([#224](https://github.com/zendesk/ruby-kafka/pull/224)).
- Add an API for fetching the last offset in a partition ([#232](https://github.com/zendesk/ruby-kafka/pull/232)).

## v0.3.9

- Improve the default durability setting. The producer setting `required_acks` now defaults to `:all` ([#210](https://github.com/zendesk/ruby-kafka/pull/210)).
- Handle rebalances in the producer ([#196](https://github.com/zendesk/ruby-kafka/pull/196)). *Mpampis Kostas*
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

- Fix bug that caused the async producer to not work with Unicorn ([#166](https://github.com/zendesk/ruby-kafka/pull/166)).
- Fix bug that caused committed consumer offsets to be lost ([#167](https://github.com/zendesk/ruby-kafka/pull/167)).
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
