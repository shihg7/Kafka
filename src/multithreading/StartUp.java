package multithreading;

import multithreading.consumer.ConsumerManager;
import multithreading.producer.ProducerManager;

public class StartUp {
    public static void main(String[] args) {
        Thread threadSend = new Thread(() -> {
            System.out.println("Start Producer Manager");
            int threadCountProd = 10;
//              int threadCountProd = 1;

            ProducerManager producerManager = new ProducerManager(threadCountProd);
            producerManager.produceMessages();
        });
        Thread threadReceive = new Thread(() -> {
            System.out.println("Start Consumer Manager");
            int threadCountCons = 5;
//              int threadCountCons = 1;
            ConsumerManager consumerManager = new ConsumerManager(threadCountCons);
            consumerManager.consumeMessages();
        });

        threadSend.start();
        threadReceive.start();
    }
}
