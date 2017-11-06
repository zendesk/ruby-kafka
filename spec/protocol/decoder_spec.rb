describe Kafka::Protocol::Decoder do
  describe "#read" do
    it "reads the specified number of bytes" do
      data = "helloworld"
      decoder = Kafka::Protocol::Decoder.from_string(data)

      expect(decoder.read(5)).to eq "hello"
    end

    it "raises EOFError if not all the data could be read" do
      data = "hell"
      decoder = Kafka::Protocol::Decoder.from_string(data)

      expect { decoder.read(5) }.to raise_exception(EOFError)
    end

    context "when there is no more data in the stream" do
      it "raises EOFError" do
        data = ""
        decoder = Kafka::Protocol::Decoder.from_string(data)

        expect { decoder.read(5) }.to raise_exception(EOFError)
      end

      context "when trying to read 0 bytes" do
        it "does not attempt to read data from IO object and returns empty string" do
          io = double
          allow(io).to receive(:read)
          decoder = Kafka::Protocol::Decoder.new(io)

          expect(decoder.read(0)).to eq ""
          expect(io).to_not have_received(:read)
        end
      end
    end
  end
end
