# frozen_string_literal: true

describe "Producer API", functional: true do
  let(:producer) { kafka.producer(max_retries: 3, retry_backoff: 1) }

  after do
    producer.shutdown
  end

  let!(:topic) { create_random_topic(num_partitions: 3) }

  example "setting a create_time value" do
    timestamp = Time.now

    producer.produce("hello", topic: topic, partition: 0, create_time: timestamp)
    producer.deliver_messages

    message = kafka.fetch_messages(topic: topic, partition: 0, offset: :earliest).last

    expect(message.create_time.to_i).to eq timestamp.to_i
  end

  example "writing messages using the buffered producer" do
    value1 = rand(10_000).to_s
    value2 = rand(10_000).to_s

    producer.produce(value1, key: "x", topic: topic, partition: 0)
    producer.produce(value2, key: "y", topic: topic, partition: 1)

    producer.deliver_messages

    message1 = kafka.fetch_messages(topic: topic, partition: 0, offset: :earliest).last
    message2 = kafka.fetch_messages(topic: topic, partition: 1, offset: :earliest).last

    expect(message1.value).to eq value1
    expect(message2.value).to eq value2
  end

  example "having the producer assign partitions based on partition keys" do
    producer.produce("hello1", key: "x", topic: topic, partition_key: "xk")
    producer.produce("hello2", key: "y", topic: topic, partition_key: "yk")

    producer.deliver_messages
  end

  example "having the producer assign partitions based on message keys" do
    producer.produce("hello1", key: "x", topic: topic)
    producer.produce("hello2", key: "y", topic: topic)

    producer.deliver_messages
  end

  example "omitting message keys entirely" do
    producer.produce("hello1", topic: topic)
    producer.produce("hello2", topic: topic)

    producer.deliver_messages
  end

  example "writing to a an explicit partition of a topic that doesn't yet exist" do
    topic = "topic#{SecureRandom.uuid}"

    producer = kafka.producer(max_retries: 10, retry_backoff: 1)
    producer.produce("hello", topic: topic, partition: 0)
    producer.deliver_messages

    expect(producer.buffer_size).to eq 0

    messages = kafka.fetch_messages(topic: topic, partition: 0, offset: 0)

    expect(messages.last.value).to eq "hello"
  end

  example "writing to a an unspecified partition of a topic that doesn't yet exist" do
    topic = "topic#{SecureRandom.uuid}"

    producer = kafka.producer(max_retries: 10, retry_backoff: 1)
    producer.produce("hello", topic: topic)
    producer.deliver_messages

    expect(producer.buffer_size).to eq 0

    messages = kafka.fetch_messages(topic: topic, partition: 0, offset: 0)

    expect(messages.last.value).to eq "hello"
  end

  example 'support record headers' do
    topic = "topic#{SecureRandom.uuid}"

    producer = kafka.producer(max_retries: 10, retry_backoff: 1)
    producer.produce(
      "hello", topic: topic,
      headers: { hello: 'World', 'greeting' => 'is great', bye: 1, love: nil }
    )
    producer.produce(
      "hello2", topic: topic,
      headers: { 'other' => 'headers' }
    )
    producer.produce("hello3", topic: topic, headers: {})
    producer.produce("hello4", topic: topic)

    producer.deliver_messages

    expect(producer.buffer_size).to eq 0

    messages = kafka.fetch_messages(topic: topic, partition: 0, offset: 0)

    expect(messages[0].value).to eq "hello"
    expect(messages[0].headers).to eql(
      'hello' => 'World',
      'greeting' => 'is great',
      'bye' => '1',
      'love' => ''
    )

    expect(messages[1].value).to eq "hello2"
    expect(messages[1].headers).to eql(
      'other' => 'headers'
    )

    expect(messages[2].value).to eq "hello3"
    expect(messages[2].headers).to eql({})

    expect(messages[3].value).to eq "hello4"
    expect(messages[3].headers).to eql({})
  end

  example "producing messages with weird characters" do
    data = <<-DATA
      iOS(Apple iPhone)에서 산타토익 App으로 기간제(2개월, 6개월) 상품을 구매 후 환불을 원하시는 경우 본 문서를 참고하십시오. 기간제 상품은 In App 결제 상품으로 Apple 계정에 등록된 청구 정보로 청구됩니다.

      1. 환불 정책

      기간제(2개월, 6개월)을 구매하신 경우 In App 결제로 환불은 Apple사의 이용약관을 따릅니다. 이에따라 당사에서는 직접적인 환불 처리를 할 수 없어 Apple 사로 환불을 요청해주시기 바랍니다.

       

      2. 환불 요청 절차

      아래의 방법으로 환불 요청을 진행해주시기 바랍니다.(Apple 사의 이용약관에 따라 환불이 되지 않을 수 있습니다.)

      문제 신고 페이지에서 신고

      Apple의 문제 신고페이지에 접속하세요

      로그인을 하세요

      산타토익을 찾아 신고버튼을 누르세요

      알맞은 환불사유를 클릭하세요

       

      iTunes 지원 페이지에서 신고

      애플의 itunes 지원 페이지에 접속하세요

      접속 후 iphone 메뉴를 클릭하세요

      iTunes Store. App store 메뉴를 클릭하세요

      주제가 목록에 없음 항목을 클릭하세요

      문제 설명 입력 내용에 환불 내용의 제목을 적어주세요 (앱 내 구입 결제 환불)

      어떤 방법으로 도움을 받으시겠습니까? 항목 중 이메일 클릭

      이메일 입력 폼의 필수 항목을 기입해주세요.

       

      세부 사항 - 필수 입력 내용

      장비: 사용하고 계시는 아이폰 기종

      앱: 산타토익

      환불사유: 어떠한 이유로 환불을 요청하는지 간략하게 적어주세요

      주문번호: 환불이 필요한 앱 구매의 주문번호 (구매내역 확인 방법)

       

      *환불 신청 및 절차에 관해 문의가 필요하시다면 Apple사의 고객센터(080-333-4000)로 문의 바랍니다.

       

      3. 환불 완료 후 추가 절차 안내

      Apple사의 환불 처리가 완료되면 반드시 당사로 아래의 방법 중 한가지로 연락을 주셔야 합니다. 그렇지 않을 경우 회원님의 서비스 이용에 문제가 발생 할 수 있습니다.

      지원 이메일 발송

        산타토익 App 실행 > 내설정 > 1:1 문의하기 '기능 오류 신고' 로 Apple사 환불 완료 여부를 전달해주시기 바랍니다.  

      고객센터로 전화

        산타토익 고객센터(02-795-1221)로 전화주시어 환불 결과를 전달해주시기 바랍니다.
    DATA

    expect {
      producer.produce(data, topic: topic, partition: 0)
      producer.deliver_messages
    }.to_not raise_exception
  end
end
