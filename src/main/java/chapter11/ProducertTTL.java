package chapter11;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * 代码清单11-1
 * Created by 朱小厮 on 2018/8/12.
 */
public class ProducertTTL {
    public static final String brokerList = "localhost:9092";
    public static final String topic = "topic-demo";

    public static void main(String[] args)
            throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("bootstrap.servers", brokerList);

        KafkaProducer<String, String> producer =
                new KafkaProducer<>(properties);

        ProducerRecord<String, String> record1 =
                new ProducerRecord<>(topic, 0, System.currentTimeMillis(),
                        null, "msg_ttl_1", new RecordHeaders().add(new RecordHeader("ttl",
                        BytesUtils.longToBytes(20))));
        ProducerRecord<String, String> record2 = //超时的消息
                new ProducerRecord<>(topic, 0, System.currentTimeMillis() - 5 * 1000,
                        null, "msg_ttl_2", new RecordHeaders().add(new RecordHeader("ttl",
                        BytesUtils.longToBytes(5))));
        ProducerRecord<String, String> record3 =
                new ProducerRecord<>(topic, 0, System.currentTimeMillis(),
                        null, "msg_ttl_3", new RecordHeaders().add(new RecordHeader("ttl",
                        BytesUtils.longToBytes(30))));
        producer.send(record1).get();
        producer.send(record2).get();
        producer.send(record3).get();
    }

}


