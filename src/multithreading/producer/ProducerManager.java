package multithreading.producer;

public class ProducerManager {
    private int producerCnt;
    private static final int THRESHOLD = 100;
    public ProducerManager(int producerCnt) {
        if (producerCnt > 0 & producerCnt <= THRESHOLD) {
            this.producerCnt = producerCnt;
        } else {
            System.out.println(String.format("com.huawei.demo.producer count out of bound:%d", producerCnt));
            this.producerCnt = 1;
        }
    }

    public void produceMessages() {
        for (int i = 0; i < producerCnt; ++i) {
            Thread thread = new Thread(new DemoProducer(i));
            thread.start();
        }
        System.out.println("All producer threads started");
    }
}
