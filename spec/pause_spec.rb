# frozen_string_literal: true

require "ostruct"

describe Kafka::Pause do
  let(:clock) { OpenStruct.new(now: 30) }
  let(:pause) { Kafka::Pause.new(clock: clock) }

  describe "#paused?" do
    it "returns true if we're paused" do
      pause.pause!

      expect(pause.paused?).to eq true
    end

    it "returns false if we're not paused" do
      pause.pause!
      pause.resume!

      expect(pause.paused?).to eq false
    end
  end

  describe "#expired?" do
    it "returns false if no timeout was specified" do
      pause.pause!
      expect(pause.expired?).to eq false
    end

    it "returns false if the timeout has not yet passed" do
      pause.pause!(timeout: 10)

      clock.now = 39

      expect(pause.expired?).to eq false
    end

    it "returns true if the timeout has passed" do
      pause.pause!(timeout: 10)

      clock.now = 40

      expect(pause.expired?).to eq true
    end

    context "with exponential backoff" do
      it "doubles the timeout with each attempt" do
        pause.pause!(timeout: 10, exponential_backoff: true)
        pause.resume!
        pause.pause!(timeout: 10, exponential_backoff: true)
        pause.resume!
        pause.pause!(timeout: 10, exponential_backoff: true)

        expect(pause.expired?).to eq false

        clock.now += 10 + 20 + 40

        expect(pause.expired?).to eq true
        expect(pause.expired?).to eq true
      end

      it "never pauses for more than the max timeout" do
        pause.pause!(timeout: 10, max_timeout: 30, exponential_backoff: true)
        pause.resume!
        pause.pause!(timeout: 10, max_timeout: 30, exponential_backoff: true)
        pause.resume!
        pause.pause!(timeout: 10, max_timeout: 30, exponential_backoff: true)

        clock.now += 29

        expect(pause.expired?).to eq false

        clock.now += 1

        expect(pause.expired?).to eq true
      end
    end
  end
end
