package org.example;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaProducerTest {
    public static void main(String[] args) throws InterruptedException {
        String bootstrapServers = "localhost:9092";
        String topic = "test-topic";

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("acks", "1");
        // props.put("compression.type", "lz4");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        int totalMessages = 100000;
        CountDownLatch latch = new CountDownLatch(totalMessages);
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < totalMessages; i++) {
            String key = String.valueOf(i);
            String value = "message-" + i;

            producer.send(new ProducerRecord<>(topic, key, value), (metadata, exception) -> {
                if (exception != null) {
                    System.err.printf("❌ Send failed for key=%s: %s\n", key, exception.getMessage());
                }
                latch.countDown();
            });

            if (i % 10_000 == 0) {
                System.out.printf("🔄 Sent %d messages...\n", i);
            }

            // Optional: 쓰로틀 걸고 싶으면 아래 주석 해제
            // Thread.sleep(1);
        }

        latch.await(); // 모든 메시지 전송 완료 대기
        producer.flush();
        producer.close();

        long duration = System.currentTimeMillis() - startTime;
        System.out.printf("✅ 총 %d개 메시지 전송 완료 (%d ms)\n", totalMessages, duration);
    }
}
