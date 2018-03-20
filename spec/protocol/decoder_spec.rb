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

    it "raises EOFError if there is not enough data left in the stream" do
      data = ""
      decoder = Kafka::Protocol::Decoder.from_string(data)

      expect { decoder.read(5) }.to raise_exception(EOFError)
    end

    it "returns an empty string when trying to read zero bytes" do
      io = StringIO.new("")
      decoder = Kafka::Protocol::Decoder.new(io)

      expect(decoder.read(0)).to eq ""
    end
  end

  describe '#varints' do
    context 'data = 0' do
      it do
        io = new_io_from_binaries("00000000")
        decoder = Kafka::Protocol::Decoder.new(io)

        expect(decoder.varints).to eq 0
      end
    end

    context 'data is stored in 1 group' do
      it do
        io = new_io_from_binaries("00001010")
        decoder = Kafka::Protocol::Decoder.new(io)

        expect(decoder.varints).to eq 10
      end
    end

    context 'data exceeds max of 1 group' do
      it do
        io = new_io_from_binaries("01111111")
        decoder = Kafka::Protocol::Decoder.new(io)

        expect(decoder.varints).to eq 127
      end
    end

    context 'data is stored in 2 groups' do
      it do
        io = new_io_from_binaries("10101100", "00000010")
        decoder = Kafka::Protocol::Decoder.new(io)

        expect(decoder.varints).to eq 300
      end
    end

    context 'data exceeds the max of 2 groups' do
      it do
        io = new_io_from_binaries("11111111", "01111111")
        decoder = Kafka::Protocol::Decoder.new(io)

        expect(decoder.varints).to eq 16383
      end
    end

    context 'data is stored in 5 groups' do
      it do
        io = new_io_from_binaries("11110010", "10010000", "10000000", "10011100", "00100101")
        decoder = Kafka::Protocol::Decoder.new(io)

        expect(decoder.varints).to eq 9990834290
      end
    end
  end
end

def new_io_from_binaries(*binaries)
  str = binaries.map { |binary| [binary.to_i(2)].pack("C") }.join("")
  StringIO.new(str)
end
