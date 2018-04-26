describe Kafka::Pause do
  describe "#paused?" do
    let(:clock) { double(:clock, now: 30) }
    let(:pause) { Kafka::Pause.new(clock: clock) }

    it "returns true if no timeout was specified" do
      pause.pause!
      expect(pause.paused?).to eq true
    end

    it "returns true if the timeout has not yet passed" do
      pause.pause!(timeout: 10)

      allow(clock).to receive(:now) { 39 }

      expect(pause.paused?).to eq true
    end

    it "returns false if the timeout has passed" do
      pause.pause!(timeout: 10)

      allow(clock).to receive(:now) { 40 }

      expect(pause.paused?).to eq false
    end

    it "doubles the timeout when a pause is renewed" do
      pause.pause!(timeout: 10)

      pause.resume!

      pause.pause!(timeout: 10)

      allow(clock).to receive(:now) { 50 }
      expect(pause.paused?).to eq false
    end
  end
end
