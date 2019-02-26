package chapter3;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

/**
 * 代码清单3-7
 * Created by 朱小厮 on 2018/8/19.
 */
public class SeekToEnd {
    public static final String brokerList = "localhost:9092";
    public static final String topic = "topic-demo";
    public static final String groupId = "group.demo";

    public static Properties initConfig() {
        Properties props = new Properties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return props;
    }

    public static void main(String[] args) {
        Properties props = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        Set<TopicPartition> assignment = new HashSet<>();
        while (assignment.size() == 0) {
            consumer.poll(Duration.ofMillis(100));
            assignment = consumer.assignment();
        }
        Map<TopicPartition, Long> offsets = consumer.endOffsets(assignment);
        for (TopicPartition tp : assignment) {
//            consumer.seek(tp, offsets.get(tp));
            consumer.seek(tp, offsets.get(tp) + 1);
        }
        System.out.println(assignment);
        System.out.println(offsets);

        while (true) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));
            //consume the record.
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.offset() + ":" + record.value());
            }
        }

    }
}
