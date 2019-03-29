import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {

        Properties properties = new Properties();
        /**
         *bootstrap.server用于建立到Kafka集群的初始连接的主机/端口对的列表，如果有两台以上的机器，逗号分隔
         */
        properties.put("bootstrap.servers", "localhost:9092");


        /**
         * 如果 acks=0，生产者在成功写入消息之前不会等待任何来自服务器的响应。也就是说，如果当中
         * 出现了问题，导致服务器没有收到消息，那么生产者就无从得知，消息也就丢失了。不过，因为
         * 生产者不需要等待服务器的响应，所以它可以以网络能够支持的最大速度发送消息，从而达到很
         * 高的吞吐量。
         *
         * 如果 acks=1，只要集群的首领节点收到消息，生产者就会收到一个来自服务器的成功响应。如果
         * 消息无法到达首领节点（比如首领节点崩溃，新的首领还没有被选举出来），生产者会收到一个
         * 错误响应，为了避免数据丢失，生产者会重发消息。不过，如果一个没有收到消息的节点成为新
         * 首领，消息还是会丢失。这个时候的吞吐量取决于使用的是同步发送还是异步发送。如果让发送
         * 客户端等待服务器的响应（通过调用 Future 对象的 get() 方法），显然会增加延迟（在网络上
         * 传输一个来回的延迟）。如果客户端使用回调，延迟问题就可以得到缓解，不过吞吐量还是会受
         * 发送中消息数量的限制（比如，生产者在收到服务器响应之前可以发送多少个消息）。
         *
         * 如果 acks=all，只有当所有参与复制的节点全部收到消息时，生产者才会收到一个来自服务器的
         * 成功响应。这种模式是最安全的，它可以保证不止一个服务器收到消息，就算有服务器发生崩
         * 溃，整个集群仍然可以运行（第 5 章将讨论更多的细节）。不过，它的延迟比 acks=1 时更高，
         * 因为我们要等待不只一个服务器节点接收消息。
         *
         */
        properties.put("acks", "all");


        /**
         * 设置成大于0将导致客户端重新发送任何发送失败的记录
         *
         */
        properties.put("retries", 0);


        /**
         * 当有多个消息需要被发送到同一个分区时，生产者会把它们放在同一个批次里。该参数指定了一个批次
         * 可以使用的内存大小，按照字节数计算（而不是消息个数）。当批次被填满，批次里的所有消息会被发
         * 送出去。不过生产者并不一定都会等到批次被填满才发送，半满的批次，甚至只包含一个消息的批次也
         * 有可能被发送。所以就算把批次大小设置得很大，也不会造成延迟，只是会占用更多的内存而已。但如
         * 果设置得太小，因为生产者需要更频繁地发送消息，会增加一些额外的开销。
         *
         * 16384字节是默认设置的批处理的缓冲区
         */
        properties.put("batch.size", 16384);


        /**
         * 该参数指定了生产者在发送批次之前等待更多消息加入批次的时间。KafkaProducer 会在批次填满或
         * linger.ms 达到上限时把批次发送出去。默认情况下，只要有可用的线程，生产者就会把消息发送出
         * 去，就算批次里只有一个消息。把 linger.ms 设置成比 0 大的数，让生产者在发送批次之前等待一会
         * 儿，使更多的消息加入到这个批次。虽然这样会增加延迟，但也会提升吞吐量（因为一次性发送更多的
         * 消息，每个消息的开销就变小了）。
         */
        properties.put("linger.ms", 1);


        /**
         * 该参数用来设置生产者内存缓冲区的大小，生产者用它缓冲要发送到服务器的消息。如果应用程序发送
         *消息的速度超过发送到服务器的速度，会导致生产者空间不足。这个时候， send() 方法调用要么被阻
         *塞，要么抛出异常，取决于如何设置 block.on.buffer.full 参数
         */

        properties.put("buffer.memory", 33554432);


        /**
         * 序列化类型。
         * kafka是以键值对的形式发送到kafka集群的，其中key是可选的，value可以是任意类型，Message再被发送到kafka之前，Producer需要
         * 把不同类型的消息转化成二进制类型。
         */
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = null;
        try {
            producer = new KafkaProducer<String, String>(properties);
            for (int i = 0; i < 5000; i++) {
                String msg = "Message " + i;
                producer.send(new ProducerRecord<String, String>("test1", msg));
                System.out.println("Sent:" + msg);
            }
        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            producer.close();
        }

    }
}
