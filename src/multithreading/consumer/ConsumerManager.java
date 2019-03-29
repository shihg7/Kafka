package multithreading.consumer;

public class ConsumerManager {
    private int consumerCount;
    private static final int THRESHOLD = 100;

    public ConsumerManager(int consumerCount) {
        if (consumerCount > 0 & consumerCount <= THRESHOLD) {
            this.consumerCount = consumerCount;
        } else {
            System.out.println(String.format("consumer count out of bound:%d", consumerCount));
            this.consumerCount = 1;
        }
    }

    public void consumeMessages() {
        for (int i = 0; i < consumerCount; ++i) {
            Thread thread = new Thread(new DemoConsumer());
            thread.start();
        }
        System.out.println("All com.huawei.demo.consumer threads started");
    }
}
