package org.example;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaConsumerTest {
    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";
        String topic = "test-topic";
        String groupId = "test-group-" + System.currentTimeMillis();

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", groupId);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        System.out.println("🎯 Consumer 시작: " + sdf.format(new Date()));

                long totalLatency = 0;
        int messageCount = 0;

        long firstReceiveTime = 0;
        long lastReceiveTime = 0;

        final int totalMessages = 131072 * 1;  // Producer와 동일한 값

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(java.time.Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                long receiveTime = System.currentTimeMillis();
                String value = record.value();

                if (!value.contains("sendTime=")) {
                    System.out.println("⚠️ Received malformed message: " + value);
                    continue;
                }

                try {
                    String sendTimeString = value.split("sendTime=")[1].split(" ")[0].trim();
                    long sendTime = Long.parseLong(sendTimeString);

                    long latency = receiveTime - sendTime;
                    totalLatency += latency;
                    messageCount++;

                    // ✅ 최초 수신 시각 기록
                    if (firstReceiveTime == 0) {
                        firstReceiveTime = receiveTime;
                    }
                    // ✅ 마지막 수신 시각 갱신
                    lastReceiveTime = receiveTime;

                    if (messageCount % 10000 == 0) {
                        double avgLatency = (double) totalLatency / messageCount;
                        System.out.printf("📊 Avg Latency: %.2f ms | Processed: %d\n", avgLatency, messageCount);
                        totalLatency = 0;
                        // messageCount = 0;
                    }

                    // ✅ 모든 메시지 수신 완료 시 총 시간 출력
                    if (messageCount == totalMessages) {
                        long totalReceiveDuration = lastReceiveTime - firstReceiveTime;
                        System.out.printf("🎉 총 %d개 메시지 수신 완료 (걸린 시간: %d ms)\n", totalMessages, totalReceiveDuration);
                        System.exit(0);  // 수신 완료 후 종료
                    }

                } catch (Exception e) {
                    System.out.println("❌ Error parsing message: " + value);
                    e.printStackTrace();
                }
            }
        }

    }
}
