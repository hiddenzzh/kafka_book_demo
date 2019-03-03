package chapter9;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.util.stream.Collectors.toList;

/**
 * 代码清单9-1
 * Created by 朱小厮的博客 on 2018/10/23.
 */
public class CheckBeginingOffset {
    public static void main(String[] args) {
    KafkaConsumer<String, String> kafkaConsumer = createNewConsumer();
    List<PartitionInfo> partitions = kafkaConsumer.partitionsFor("topic-monitor");
    List<TopicPartition> tpList = partitions.stream()
            .map(pInfo -> new TopicPartition(pInfo.topic(), pInfo.partition()))
            .collect(toList());
    Map<TopicPartition, Long> beginningOffsets =
            kafkaConsumer.beginningOffsets(tpList);
    System.out.println(beginningOffsets);
    }

    public static KafkaConsumer<String,String> createNewConsumer(){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "CheckBeginingOffset");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }
}
