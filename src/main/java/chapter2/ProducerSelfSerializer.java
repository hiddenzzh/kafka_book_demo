package chapter2;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * 代码清单2-4
 * Created by 朱小厮 on 2018/7/26.
 */
public class ProducerSelfSerializer {
    public static final String brokerList = "localhost:9092";
    public static final String topic = "topic-demo";

    public static void main(String[] args)
            throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                CompanySerializer.class.getName());
//        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
//                ProtostuffSerializer.class.getName());
        properties.put("bootstrap.servers", brokerList);

        KafkaProducer<String, Company> producer =
                new KafkaProducer<>(properties);
        Company company = Company.builder().name("hiddenkafka")
                .address("China").build();
//        Company company = Company.builder().name("hiddenkafka")
//                .address("China").telphone("13000000000").build();
        ProducerRecord<String, Company> record =
                new ProducerRecord<>(topic, company);
        producer.send(record).get();
    }
}
