describe Kafka::Protocol::Encoder do
  let(:io) { StringIO.new("") }

  describe '#write_varints' do
    context 'data = 0' do
      it do
        encoder = Kafka::Protocol::Encoder.new(io)
        encoder.write_varints(0)
        expect(binaries_in_io(io)).to eq ["00000000"]
      end
    end

    context 'data is stored in 1 group' do
      it do
        encoder = Kafka::Protocol::Encoder.new(io)
        encoder.write_varints(10)
        expect(binaries_in_io(io)).to eq ["00001010"]
      end
    end

    context 'data exceeds max of 1 group' do
      it do
        encoder = Kafka::Protocol::Encoder.new(io)
        encoder.write_varints(127)
        expect(binaries_in_io(io)).to eq ["01111111"]
      end
    end

    context 'data is stored in 2 groups' do
      it do
        encoder = Kafka::Protocol::Encoder.new(io)
        encoder.write_varints(300)
        expect(binaries_in_io(io)).to eq ["10101100", "00000010"]
      end
    end

    context 'data exceeds the max of 2 groups' do
      it do
        encoder = Kafka::Protocol::Encoder.new(io)
        encoder.write_varints(16383)
        expect(binaries_in_io(io)).to eq ["11111111", "01111111"]
      end
    end

    context 'data is stored in 5 groups' do
      it do
        encoder = Kafka::Protocol::Encoder.new(io)
        encoder.write_varints(9990834290)
        expect(binaries_in_io(io)).to eq ["11110010", "10010000", "10000000", "10011100", "00100101"]
      end
    end
  end
end

def binaries_in_io(io)
  io.string.bytes.map { |byte| byte.to_s(2).rjust(8, '0') }
end
