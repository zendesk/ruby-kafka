describe Kafka::Protocol::Encoder do
  let(:io) { StringIO.new("") }

  describe '#write_varint' do
    context 'data = 0' do
      it do
        encoder = Kafka::Protocol::Encoder.new(io)
        encoder.write_varint(0)
        expect(binaries_in_io(io)).to eq ["00000000"]
      end
    end

    context 'data is positive' do
      context 'data is stored in 1 group' do
        it do
          encoder = Kafka::Protocol::Encoder.new(io)
          encoder.write_varint(10)
          expect(binaries_in_io(io)).to eq ["00010100"]
        end
      end

      context 'data exceeds max of 1 group' do
        it do
          encoder = Kafka::Protocol::Encoder.new(io)
          encoder.write_varint(63)
          expect(binaries_in_io(io)).to eq ["01111110"]
        end
      end

      context 'data is stored in 2 groups' do
        it do
          encoder = Kafka::Protocol::Encoder.new(io)
          encoder.write_varint(300)
          expect(binaries_in_io(io)).to eq ["11011000", "00000100"]
        end
      end

      context 'data is stored in 3 groups' do
        it do
          encoder = Kafka::Protocol::Encoder.new(io)
          encoder.write_varint(215233)
          expect(binaries_in_io(io)).to eq ["10000010", "10100011", "00011010"]
        end
      end
    end

    context 'data is negative' do
      context 'data is stored in 1 group' do
        it do
          encoder = Kafka::Protocol::Encoder.new(io)
          encoder.write_varint(-10)
          expect(binaries_in_io(io)).to eq ["00010011"]
        end
      end

      context 'data exceeds max of 1 group' do
        it do
          encoder = Kafka::Protocol::Encoder.new(io)
          encoder.write_varint(-63)
          expect(binaries_in_io(io)).to eq ["01111101"]
        end
      end

      context 'data is stored in 2 groups' do
        it do
          encoder = Kafka::Protocol::Encoder.new(io)
          encoder.write_varint(-300)
          expect(binaries_in_io(io)).to eq ["11010111", "00000100"]
        end
      end

      context 'data is stored in 3 groups' do
        it do
          encoder = Kafka::Protocol::Encoder.new(io)
          encoder.write_varint(-215233)
          expect(binaries_in_io(io)).to eq ["10000001", "10100011", "00011010"]
        end
      end
    end
  end
end

def binaries_in_io(io)
  io.string.bytes.map { |byte| byte.to_s(2).rjust(8, '0') }
end
