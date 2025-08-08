package org.example;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaProducerTest {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "test-topic";
    private static final int TOTAL_MESSAGES = 131072;  // 각 스레드당 전송 수
    private static final int NUM_THREADS = 1;          // 스레드 개수

    public static void main(String[] args) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(NUM_THREADS * TOTAL_MESSAGES);
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < NUM_THREADS; i++) {
            Thread t = new Thread(new ProducerWorker(i, latch));
            t.start();
        }

        latch.await();
        long duration = System.currentTimeMillis() - startTime;
        double throughput = (double) (NUM_THREADS * TOTAL_MESSAGES) / (duration / 1000.0);

        System.out.printf("✅ 총 %d개 메시지 전송 완료 (%d ms)\n", NUM_THREADS * TOTAL_MESSAGES, duration);
        System.out.printf("⚡ 처리량: %.2f messages/sec\n", throughput);
    }

    static class ProducerWorker implements Runnable {
        private final int workerId;
        private final CountDownLatch latch;

        ProducerWorker(int workerId, CountDownLatch latch) {
            this.workerId = workerId;
            this.latch = latch;
        }

        @Override
        public void run() {
            Properties props = new Properties();
            props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
            props.put("acks", "1");
            // props.put("compression.type", "lz4");
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            KafkaProducer<String, String> producer = new KafkaProducer<>(props);

            for (int i = 0; i < TOTAL_MESSAGES; i++) {
                long sendTime = System.currentTimeMillis();
                String key = "worker-" + workerId + "-msg-" + i;
                String value = "worker=" + workerId + " message-" + i + " sendTime=" + sendTime;

                producer.send(new ProducerRecord<>(TOPIC, key, value), (metadata, exception) -> {
                    if (exception != null) {
                        System.err.printf("❌ Send failed for key=%s: %s\n", key, exception.getMessage());
                    }
                    latch.countDown();
                });

                // Optional: Thread.sleep(1);
            }

            producer.flush();
            producer.close();
        }
    }
}
