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

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(java.time.Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                long receiveTime = System.currentTimeMillis();
                String value = record.value();

                // ✅ 메시지에 "sendTime=" 포함되어 있어야 함
                if (!value.contains("sendTime=")) {
                    System.out.println("⚠️ Received malformed message: " + value);
                    continue;
                }

                try {
                    // ✅ sendTime=밀리초 포맷 파싱
                    String sendTimeString = value.split("sendTime=")[1].split(" ")[0].trim();
                    long sendTime = Long.parseLong(sendTimeString);

                    long latency = receiveTime - sendTime;
                    totalLatency += latency;
                    messageCount++;

                    if (messageCount % 1000 == 0) {
                        double avgLatency = (double) totalLatency / messageCount;
                        System.out.printf("📊 Avg Latency: %.2f ms | Processed: %d\n", avgLatency, messageCount);
                        totalLatency = 0;
                        messageCount = 0;
                    }
                } catch (Exception e) {
                    System.out.println("❌ Error parsing message: " + value);
                    e.printStackTrace();
                }
            }
        }
    }
}
