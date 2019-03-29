package multithreading.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class DemoProducer implements Runnable {
    private static Properties props;
    public static final String TOPIC = "wxymsg";
    private static final int REQUESTS = 10000;
    private int id;

    static {
        props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("acks", "all");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }

    public DemoProducer(int id) {
        this.id = id;
    }

    @Override
    public void run() {
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < REQUESTS; ++i) {
            String key = id+","+i;
            String value = String.format("This is message from thread-%d:%dth", id, i);
            producer.send(new ProducerRecord<String, String>(TOPIC, key, value));
            System.out.println("Send:" + key + "->" + value);
        }

        producer.close();
        System.out.println("DemoProducer end");
    }
}
