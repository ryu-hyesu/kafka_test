package org.example;
import java.util.concurrent.ExecutionException;

public class App {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // ✅ Consumer 먼저 실행 (메인 스레드)
        Thread consumerThread = new Thread(() -> KafkaConsumerTest.main(new String[]{}));
        consumerThread.start();

        // ✅ 약간의 지연 후 Producer 실행 (메시지를 놓치지 않도록)
        try {
            Thread.sleep(5000);  // 5초 대기
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // ✅ Producer 실행
        KafkaProducerTest.main(new String[]{});
    }
}
