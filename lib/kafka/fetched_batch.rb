# frozen_string_literal: true

module Kafka

  # An ordered sequence of messages fetched from a Kafka partition.
  class FetchedBatch
    # @return [String]
    attr_reader :topic

    # @return [Integer]
    attr_reader :partition

    # @return [Integer]
    attr_reader :last_offset

    # @return [Integer]
    attr_reader :leader_epoch

    # @return [Integer] the offset of the most recent message in the partition.
    attr_reader :highwater_mark_offset

    # @return [Array<Kafka::FetchedMessage>]
    attr_accessor :messages

    def initialize(topic:, partition:, highwater_mark_offset:, messages:, last_offset: nil, leader_epoch: nil)
      @topic = topic
      @partition = partition
      @highwater_mark_offset = highwater_mark_offset
      @messages = messages
      @last_offset = last_offset
      @leader_epoch = leader_epoch
    end

    def empty?
      @messages.empty?
    end

    def unknown_last_offset?
      @last_offset.nil?
    end

    def first_offset
      if empty?
        nil
      else
        messages.first.offset
      end
    end

    def offset_lag
      if empty?
        0
      else
        (highwater_mark_offset - 1) - last_offset
      end
    end
  end
end
