package chapter7;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;

/**
 * 代码清单7-3
 * Created by 朱小厮 on 2018/10/18.
 */
public class TransactionConsumeTransformProduce {
    public static final String brokerList = "10.198.197.73:9092";

    public static Properties getConsumerProperties(){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "groupId");
        return props;
    }

    public static Properties getProducerProperties(){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactionalId");
        return props;
    }

    public static void main(String[] args) {
        //初始化生产者和消费者
        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(getConsumerProperties());
        consumer.subscribe(Collections.singletonList("topic-source"));
        KafkaProducer<String, String> producer =
                new KafkaProducer<>(getProducerProperties());
        //初始化事务
        producer.initTransactions();
        while (true) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));
            if (!records.isEmpty()) {
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                //开启事务
                producer.beginTransaction();
                try {
                    for (TopicPartition partition : records.partitions()) {
                        List<ConsumerRecord<String, String>> partitionRecords
                                = records.records(partition);
                        for (ConsumerRecord<String, String> record :
                                partitionRecords) {
                            //do some logical processing.
                            ProducerRecord<String, String> producerRecord =
                                    new ProducerRecord<>("topic-sink", record.key(),
                                            record.value());
                            //消费-生产模型
                            producer.send(producerRecord);
                        }
                        long lastConsumedOffset = partitionRecords.
                                get(partitionRecords.size() - 1).offset();
                        offsets.put(partition,
                                new OffsetAndMetadata(lastConsumedOffset + 1));
                    }
                    //提交消费位移
                    producer.sendOffsetsToTransaction(offsets,"groupId");
                    //提交事务
                    producer.commitTransaction();
                } catch (ProducerFencedException e) {
                    //log the exception
                    //中止事务
                    producer.abortTransaction();
                }
            }
        }
    }
}
