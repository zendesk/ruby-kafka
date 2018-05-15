# frozen_string_literal: true

describe Kafka::Protocol::Decoder do
  describe '#peek' do
    let(:decoder) { Kafka::Protocol::Decoder.from_string(data) }

    context 'io stream is empty' do
      let(:data) { "" }
      it 'returns empty array' do
        expect(decoder.peek(3, 1)).to eql([])
      end
    end

    context 'data in io stream is shorted than expected' do
      let(:data) { "12345" }

      before do
        decoder.read(1)
      end

      it 'returns partial byte array' do
        expect(decoder.peek(2, 5)).to eql([52, 53])
      end

      it 'does not change the read offset' do
        decoder.peek(2, 5)
        expect(decoder.read(1)).to eql("2")
      end
    end

    context 'read without offset' do
      let(:data) { "12345" }

      before do
        decoder.read(1)
      end

      it 'returns desired bytes array' do
        expect(decoder.peek(0, 2)).to eql([50, 51])
      end

      it 'does not change the read offset' do
        decoder.peek(0, 2)
        expect(decoder.read(1)).to eql("2")
      end
    end

    context 'peek with offset' do
      let(:data) { "12345" }

      before do
        decoder.read(1)
      end

      it 'returns desired bytes array' do
        expect(decoder.peek(2, 2)).to eql([52, 53])
      end

      it 'does not change the read offset' do
        decoder.peek(2, 2)
        expect(decoder.read(1)).to eql("2")
      end
    end
  end

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

  describe '#varint' do
    context 'data = 0' do
      it do
        io = new_io_from_binaries("00000000")
        decoder = Kafka::Protocol::Decoder.new(io)

        expect(decoder.varint).to eq 0
      end
    end

    context 'data is positive' do
      context 'data is stored in 1 group' do
        it do
          io = new_io_from_binaries("00010100")
          decoder = Kafka::Protocol::Decoder.new(io)

          expect(decoder.varint).to eq 10
        end
      end

      context 'data exceeds max of 1 group' do
        it do
          io = new_io_from_binaries("01111110")
          decoder = Kafka::Protocol::Decoder.new(io)

          expect(decoder.varint).to eq 63
        end
      end

      context 'data is stored in 2 groups' do
        it do
          io = new_io_from_binaries("11011000", "00000100")
          decoder = Kafka::Protocol::Decoder.new(io)

          expect(decoder.varint).to eq 300
        end
      end

      context 'data is stored in 3 groups' do
        it do
          io = new_io_from_binaries("10000010", "10100011", "00011010")
          decoder = Kafka::Protocol::Decoder.new(io)

          expect(decoder.varint).to eq 215233
        end
      end
    end

    context 'data is negative' do
      context 'data is stored in 1 group' do
        it do
          io = new_io_from_binaries("00010011")
          decoder = Kafka::Protocol::Decoder.new(io)

          expect(decoder.varint).to eq -10
        end
      end

      context 'data exceeds max of 1 group' do
        it do
          io = new_io_from_binaries("01111101")
          decoder = Kafka::Protocol::Decoder.new(io)

          expect(decoder.varint).to eq -63
        end
      end

      context 'data is stored in 2 groups' do
        it do
          io = new_io_from_binaries("11010111", "00000100")
          decoder = Kafka::Protocol::Decoder.new(io)

          expect(decoder.varint).to eq -300
        end
      end

      context 'data is stored in 3 groups' do
        it do
          io = new_io_from_binaries("10000001", "10100011", "00011010")
          decoder = Kafka::Protocol::Decoder.new(io)

          expect(decoder.varint).to eq -215233
        end
      end
    end
  end
end

def new_io_from_binaries(*binaries)
  str = binaries.map { |binary| [binary.to_i(2)].pack("C") }.join("")
  StringIO.new(str)
end
